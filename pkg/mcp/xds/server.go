package xds

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"istio.io/istio-mcp/pkg/mcp"
	"istio.io/pkg/log"

	"istio.io/istio-mcp/pkg/model"
)

const (
	DefaultAddr = "0.0.0.0"
	DefaultPort = 15010
)

var DefaultXdsServerOption = ServerOptions{
	Addr: DefaultAddr,
	Port: DefaultPort,
}

type ServerOptions struct {
	Addr    string
	Port    int
	IncPush bool
}

func DefaultServerOptions() *ServerOptions {
	o := DefaultXdsServerOption
	return &o
}

func ParseIntoServerOptions(urlStr string, o *ServerOptions) error {
	u, err := url.Parse(urlStr)
	if err != nil {
		return err
	}

	if h := u.Hostname(); h != "" {
		o.Addr = h
	}

	if p := u.Port(); p != "" {
		if o.Port, err = strconv.Atoi(p); err != nil {
			return err
		}
	}

	return nil
}

func ParseServerOptions(urlStr string) (*ServerOptions, error) {
	o := DefaultServerOptions()
	if err := ParseIntoServerOptions(urlStr, o); err != nil {
		return nil, err
	}
	return o, nil
}

type Server struct {
	o *ServerOptions

	// mutex used for config update scheduling (former cache update mutex)
	updateMutex sync.RWMutex
	mutex       sync.RWMutex
	serverReady bool

	// xdsClients reflect active gRPC channels, for both ADS and EDS.
	xdsClients      map[string]*Connection
	xdsClientsMutex sync.RWMutex

	// InternalGen is notified of connect/disconnect/nack on all connections
	InternalGen *InternalGen // TODO y: Handle events and urlconnections. Leave to impl.

	// Generators allow customizing the generated config, based on the client metadata.
	// Key is the generator type - will match the Generator metadata to set the per-connection
	// default generator, or the combination of Generator metadata and TypeUrl to select a
	// different generator for a type.
	// Normal istio clients use the default generator - will not be impacted by this.
	Generators map[string]XdsResourceGenerator

	pushContext *PushContext

	store               model.ConfigStore
	xdsListener         net.Listener
	clientEventHandlers []func(event mcp.ClientEvent)
}

var _ mcp.Server = &Server{}

func (s *Server) ClientsInfo() map[string]mcp.ClientDetailedInfo {
	s.xdsClientsMutex.RLock()
	xdsClients := make(map[string]*Connection, len(s.xdsClients))
	for k, v := range s.xdsClients {
		xdsClients[k] = v
	}
	s.xdsClientsMutex.RUnlock()

	ret := make(map[string]mcp.ClientDetailedInfo, len(xdsClients))
	for id, cli := range xdsClients {
		ret[id] = mcp.ClientDetailedInfo{
			ClientInfo: cli.clientInfo(),
			PushStatus: cli.PushStatus(),
		}
	}
	return ret
}

func (s *Server) RegisterClientEventHandler(h func(mcp.ClientEvent)) {
	s.clientEventHandlers = append(s.clientEventHandlers, h)
}

func (s *Server) DeltaAggregatedResources(server discovery.AggregatedDiscoveryService_DeltaAggregatedResourcesServer) error {
	return status.Errorf(codes.Unimplemented, "not implemented")
}

func NewServer(o *ServerOptions) *Server {
	ret := &Server{
		o:           o,
		xdsClients:  map[string]*Connection{},
		Generators:  map[string]XdsResourceGenerator{},
		pushContext: &PushContext{},
	}

	ret.initGenerators()

	return ret
}

func (s *Server) SetConfigStore(store model.ConfigStore) {
	s.store = store
	s.pushContext.ConfigStore = store
}

func (s *Server) NotifyPush(req *mcp.PushRequest) {
	ev := &Event{push: s.globalPushContext()}
	if req != nil {
		if len(req.RevChangeConfigs) > 0 {
			if ev.configsUpdated == nil {
				ev.configsUpdated = map[model.ConfigKey]struct{}{}
			}
		}
		for k := range req.RevChangeConfigs {
			ev.configsUpdated[k] = struct{}{}
		}
	}

	s.xdsClientsMutex.RLock()
	xdsClients := make([]*Connection, 0, len(s.xdsClients))
	for _, c := range s.xdsClients {
		xdsClients = append(xdsClients, c)
	}
	s.xdsClientsMutex.RUnlock()

	var newNotified int
	for _, con := range xdsClients {
		if con.Notify(ev) {
			newNotified++
		}
	}

	log.Infof("Server.NotifyPush, total clients %d, new notified %d", len(xdsClients), newNotified)
}

// Register adds the ADS and EDS handles to the grpc server
func (s *Server) Register(rpcs *grpc.Server) {
	// Register v2 and v3 servers
	discovery.RegisterAggregatedDiscoveryServiceServer(rpcs, s)
	// TODO v2 adapter left to impl.
}

func (s *Server) Start(ctx context.Context) {
	go func() {
		addr := fmt.Sprintf("%s:%d", s.o.Addr, s.o.Port)

		lis, err := net.Listen("tcp", addr)
		if err != nil {
			log.Errorf("can not listen to xds addr %s, err %v", addr, err)
			return
		}
		gs := grpc.NewServer()
		s.Register(gs)
		reflection.Register(gs)
		s.xdsListener = lis
		go func() {
			err = gs.Serve(lis)
			if err != nil {
				log.Infoa("Serve done ", err)
			}
		}()
		return
	}()
}

func (s *Server) IsServerReady(ns string) bool {
	snap := s.store.Snapshot(ns)
	return snap != nil && snap.Version() != model.VersionNotInitialized
}

// Returns the global push context.
func (s *Server) globalPushContext() *PushContext {
	s.updateMutex.RLock()
	defer s.updateMutex.RUnlock()
	return s.pushContext
}
