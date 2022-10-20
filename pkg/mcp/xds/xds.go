package xds

import (
	"errors"
	"fmt"
	"io"
	"istio.io/istio-mcp/pkg/config/schema/resource"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/google/uuid"
	uatomic "go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"istio.io/istio-mcp/pkg/features"
	"istio.io/istio-mcp/pkg/mcp"
	istiolog "istio.io/pkg/log"

	"istio.io/istio-mcp/pkg/model"
)

var (
	xdsLog = istiolog.RegisterScope("mcp-xds", "ads debugging", 0)

	// SendTimeout is the max time to wait for a ADS send to complete. This helps detect
	// clients in a bad state (not reading). In future it may include checking for ACK
	SendTimeout = features.XdsSendTimeout

	versionMutex sync.RWMutex
	// version is the timestamp of the last registry event.
	version = "0"
	// versionNum counts versions
	versionNum = uatomic.NewUint64(0)
)

func nonce(noncePrefix string) string {
	return noncePrefix + uuid.New().String()
}

// DiscoveryStream is an interface for ADS.
type DiscoveryStream interface {
	Send(*discovery.DiscoveryResponse) error
	Recv() (*discovery.DiscoveryRequest, error)
	grpc.ServerStream
}

// Event represents a config or registry event that results in a push.
type Event struct {
	// Indicate whether the push is Full Push
	full bool

	configsUpdated map[model.ConfigKey]struct{}

	// Push context to use for the push.
	push *PushContext

	// start represents the time a push was started.
	start time.Time

	// function to call once a push is finished. This must be called or future changes may be blocked.
	done func()

	noncePrefix string
}

// Connection holds information about connected client.
type Connection struct {
	// PeerAddr is the address of the client envoy, from network layer.
	PeerAddr string

	// Time of connection, for debugging
	Connect time.Time

	// ConID is the connection identifier, used as a key in the connection table.
	// Currently based on the node name and a counter.
	ConID string

	// mutex to protect changes to the node.
	// TODO: move into model.Proxy
	mu sync.RWMutex
	// node *model.Proxy
	node *Proxy

	// Sending on this channel results in a push.
	pushChannel chan *Event

	// Both ADS and SDS streams implement this interface
	stream DiscoveryStream

	// Original node metadata, to avoid unmarshal/marshal.
	// This is included in internal events.
	xdsNode *core.Node
}

func newConnection(peerAddr string, stream DiscoveryStream) *Connection {
	return &Connection{
		pushChannel: make(chan *Event, 1), // no merge.
		PeerAddr:    peerAddr,
		Connect:     time.Now(),
		stream:      stream,
	}
}

func (conn *Connection) clientInfo() mcp.ClientInfo {
	return mcp.ClientInfo{
		ClientId: conn.ConID,
		ConnId:   conn.ConID,
		Addr:     conn.PeerAddr,
	}
}

func (conn *Connection) Notify(ev *Event) bool {
	select {
	case conn.pushChannel <- ev:
		return true
	default:
		return false
	}
}

// Send with timeout
func (conn *Connection) send(res *discovery.DiscoveryResponse) error {
	done := make(chan error, 1)
	// hardcoded for now - not sure if we need a setting
	t := time.NewTimer(SendTimeout)
	go func() {
		err := conn.stream.Send(res)
		stype := GetShortType(res.TypeUrl)
		conn.mu.Lock()
		if res.Nonce != "" {
			w := conn.node.Active[stype]
			if w == nil {
				w = NewWatchedResource(res.TypeUrl)
				conn.node.Active[stype] = w
			}
			w.RecordNonceSent(res.Nonce)
			w.VersionSent = res.VersionInfo
		}
		conn.mu.Unlock()
		done <- err
	}()
	select {
	case <-t.C:
		// TODO: wait for ACK
		xdsLog.Infof("Timeout writing %s", conn.ConID)
		return errors.New("timeout sending")
	case err := <-done:
		t.Stop()
		return err
	}
}

func (conn *Connection) NonceAcked(stype string) string {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	wr := conn.node.Active[stype]
	if wr == nil {
		return ""
	}
	return wr.NonceAcked
}

func (conn *Connection) NonceSent(stype string) string {
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	wr := conn.node.Active[stype]
	if wr == nil {
		return ""
	}
	return wr.NonceSent
}

func (conn *Connection) PushStatus() map[resource.GroupVersionKind]mcp.ConfigPushStatus {
	ret := map[resource.GroupVersionKind]mcp.ConfigPushStatus{}
	conn.mu.RLock()
	defer conn.mu.RUnlock()
	for _, wr := range conn.node.Active {
		ret[resource.TypeUrlToGvk(wr.TypeUrl)] = mcp.ConfigPushStatus{
			PushVer: wr.NonceSent,
			AckVer:  wr.NonceAcked,
		}
	}
	return ret
}

// isExpectedGRPCError checks a gRPC error code and determines whether it is an expected error when
// things are operating normally. This is basically capturing when the client disconnects.
func isExpectedGRPCError(err error) bool {
	if err == io.EOF {
		return true
	}

	s := status.Convert(err)
	if s.Code() == codes.Canceled || s.Code() == codes.DeadlineExceeded {
		return true
	}
	if s.Code() == codes.Unavailable && s.Message() == "client disconnected" {
		return true
	}
	return false
}

// processRequest is handling one request. This is currently called from the 'main' thread, which also
// handles 'push' requests and close - the code will eventually call the 'push' code, and it needs more mutex
// protection. Original code avoided the mutexes by doing both 'push' and 'process requests' in same thread.
func (s *Server) processRequest(discReq *discovery.DiscoveryRequest, con *Connection) error {
	var err error

	// Based on node metadata a different generator was selected,
	// use it instead of the default behavior.
	if con.node.XdsResourceGenerator != nil {
		// Endpoints are special - will use the optimized code path.
		err = s.handleCustomGenerator(con, discReq)
		if err != nil {
			return err
		}
		return nil
	}

	switch discReq.TypeUrl {
	default:
		// Allow custom generators to work without 'generator' metadata.
		// It would be an error/warn for normal XDS - so nothing to lose.
		err = s.handleCustomGenerator(con, discReq)
		if err != nil {
			return err
		}
	}
	return nil
}

var _ discovery.AggregatedDiscoveryServiceServer = &Server{}

// StreamAggregatedResources implements the ADS interface.
func (s *Server) StreamAggregatedResources(stream discovery.AggregatedDiscoveryService_StreamAggregatedResourcesServer) error {
	// Check if server is ready to accept clients and process new requests.
	// Currently ready means caches have been synced and hence can build
	// clusters correctly. Without this check, InitContext() call below would
	// initialize with empty config, leading to reconnected Envoys loosing
	// configuration. This is an additional safety check inaddition to adding
	// cachesSynced logic to readiness probe to handle cases where kube-proxy
	// ip tables update latencies.
	// See https://github.com/istio/istio/issues/25495.
	// TODO y: improve IsServerReady
	//if !s.IsServerReady() {
	//	return errors.New("server is not ready to serve discovery information")
	//}

	ctx := stream.Context()
	peerAddr := "0.0.0.0"
	if peerInfo, ok := peer.FromContext(ctx); ok {
		peerAddr = peerInfo.Addr.String()
	}

	// TODO auth left to impl.
	//ids, err := s.authenticate(ctx)
	//if err != nil {
	//	return err
	//}
	//if ids != nil {
	//	xdsLog.Debugf("Authenticated XDS: %v with identity %v", peerAddr, ids)
	//} else {
	//	xdsLog.Debuga("Unauthenticated XDS: ", peerAddr)
	//}

	con := newConnection(peerAddr, stream)

	// Do not call: defer close(con.pushChannel). The push channel will be garbage collected
	// when the connection is no longer used. Closing the channel can cause subtle race conditions
	// with push. According to the spec: "It's only necessary to close a channel when it is important
	// to tell the receiving goroutines that all data have been sent."

	// Reading from a stream is a blocking operation. Each connection needs to read
	// discovery requests and wait for push commands on config change, so we add a
	// go routine. If go grpc adds gochannel support for streams this will not be needed.
	// This also detects close.
	var receiveError error
	reqChannel := make(chan *discovery.DiscoveryRequest, 1)
	go s.receiveThread(con, reqChannel, &receiveError)

	for {
		// Block until either a request is received or a push is triggered.
		// We need 2 go routines because 'read' blocks in Recv().
		//
		// To avoid 2 routines, we tried to have Recv() in StreamAggregateResource - and the push
		// on different short-lived go routines started when the push is happening. This would cut in 1/2
		// the number of long-running go routines, since push is throttled. The main problem is with
		// closing - the current gRPC library didn't allow closing the stream.
		select {
		case req, ok := <-reqChannel:
			if !ok {
				// Remote side closed connection or error processing the request.
				return receiveError
			}

			xdsLog.Infof("recv req %s from %s, err %v, nonce %s", req.TypeUrl, con.ConID, req.ErrorDetail, req.ResponseNonce)

			// processRequest is calling pushXXX, accessing common structs with pushConnection.
			// Adding sync is the second issue to be resolved if we want to save 1/2 of the threads.
			err := s.processRequest(req, con)
			if err != nil {
				xdsLog.Errorf("processReq for %s err detail %v nonce %s met err %s", con.ConID, req.ErrorDetail, req.ResponseNonce, err)
				return err
			}

		case pushEv := <-con.pushChannel:
			// It is called when config changes.
			// This is not optimized yet - we should detect what changed based on event and only
			// push resources that need to be pushed.

			// TODO: possible race condition: if a config change happens while the envoy
			// was getting the initial config, between LDS and RDS, the push will miss the
			// monitored 'routes'. Same for CDS/EDS interval.
			// It is very tricky to handle due to the protocol - but the periodic push recovers
			// from it.

			err := s.pushConnection(con, pushEv)
			if done := pushEv.done; done != nil {
				pushEv.done()
			}
			if err != nil {
				xdsLog.Errorf("pushConnection for %s met err %v", con.ConID, err)
				return nil
			}
		}
	}
}

func (s *Server) receiveThread(con *Connection, reqChannel chan *discovery.DiscoveryRequest, errP *error) {
	defer close(reqChannel) // indicates close of the remote side.
	firstReq := true
	for {
		req, err := con.stream.Recv()
		if err != nil {
			if isExpectedGRPCError(err) {
				xdsLog.Infof("ADS: %q %s terminated %v", con.PeerAddr, con.ConID, err)
				return
			}
			*errP = err
			xdsLog.Errorf("ADS: %q %s terminated with error: %v", con.PeerAddr, con.ConID, err)
			return
		}
		// This should be only set for the first request. The node id may not be set - for example malicious clients.
		if firstReq {
			firstReq = false
			if req.Node == nil || req.Node.Id == "" {
				*errP = errors.New("missing node ID")
				return
			}
			// TODO: We should validate that the namespace in the cert matches the claimed namespace in metadata.
			if err := s.initConnection(req.Node, con); err != nil {
				*errP = err
				return
			}
			defer func() {
				s.removeCon(con.ConID)
				ev := mcp.ClientEvent{
					Clients:   []mcp.ClientInfo{con.clientInfo()},
					EventType: mcp.ClientEventDelete,
				}
				for _, h := range s.clientEventHandlers {
					h(ev)
				}
				if s.InternalGen != nil {
					s.InternalGen.OnDisconnect(con)
				}
			}()
		}

		select {
		case reqChannel <- req:
		case <-con.stream.Context().Done():
			xdsLog.Infof("ADS: %q %s terminated with stream closed", con.PeerAddr, con.ConID)
			return
		}
	}
}

// update the node associated with the connection, after receiving a a packet from envoy, also adds the connection
// to the tracking map.
func (s *Server) initConnection(node *core.Node, con *Connection) error {
	proxy, err := s.initProxy(node)
	if err != nil {
		return err
	}

	// Based on node metadata and version, we can associate a different generator.
	// TODO: use a map of generators, so it's easily customizable and to avoid deps
	proxy.Active = map[string]*WatchedResource{}
	proxy.ActiveExperimental = map[string]*WatchedResource{}

	if proxy.Metadata.Generator != "" {
		proxy.XdsResourceGenerator = s.Generators[proxy.Metadata.Generator]
	}

	// First request so initialize connection id and start tracking it.
	con.node = proxy
	con.ConID = connectionID(node.Id)
	con.xdsNode = node

	s.addCon(con.ConID, con)

	if s.InternalGen != nil {
		s.InternalGen.OnConnect(con)
	}
	clientEv := mcp.ClientEvent{
		Clients:   []mcp.ClientInfo{con.clientInfo()},
		EventType: mcp.ClientEventAdd,
	}
	for _, h := range s.clientEventHandlers {
		h(clientEv)
	}
	return nil
}

func (s *Server) addCon(conID string, con *Connection) {
	s.xdsClientsMutex.Lock()
	defer s.xdsClientsMutex.Unlock()
	s.xdsClients[conID] = con
}

func (s *Server) removeCon(conID string) {
	s.xdsClientsMutex.Lock()
	defer s.xdsClientsMutex.Unlock()

	if _, exist := s.xdsClients[conID]; !exist {
		xdsLog.Errorf("ADS: Removing connection for non-existing node:%v.", conID)
	} else {
		delete(s.xdsClients, conID)
	}
}

// initProxy initializes the Proxy from node.
func (s *Server) initProxy(node *core.Node) (*Proxy, error) {
	meta, err := ParseMetadata(node.Metadata)
	if err != nil {
		return nil, err
	}
	proxy, err := ParseServiceNodeWithMetadata(node.Id, meta)
	if err != nil {
		return nil, err
	}
	// Update the config namespace associated with this proxy
	// proxy.ConfigNamespace = model.GetProxyConfigNamespace(proxy)

	// Discover supported IP Versions of proxy so that appropriate config can be delivered.
	proxy.DiscoverIPVersions()

	return proxy, nil
}

// Compute and send the new configuration for a connection. This is blocking and may be slow
// for large configs. The method will hold a lock on con.pushMutex.
func (s *Server) pushConnection(con *Connection, pushEv *Event) error {
	// This depends on SidecarScope updates, so it should be called after SetSidecarScope.
	if !ProxyNeedsPush(con.node, pushEv) {
		return nil
	}

	xdsLog.Infof("Pushing %v", con.ConID)

	// check version, suppress if changed.
	currentVersion := versionInfo()

	// When using Generator, the generic WatchedResource is used instead of the individual
	// 'LDSWatch', etc.
	// Each Generator is responsible for determining if the push event requires a push -
	// returning nil if the push is not needed.
	if con.node.XdsResourceGenerator != nil {
		for _, w := range con.node.Active {
			err := s.pushGeneratorV2(con, pushEv.push, currentVersion, w, pushEv.configsUpdated)
			if err != nil {
				return fmt.Errorf("pushGeneratorV2 typeurl %s version %s mer err %v", w.TypeUrl, currentVersion, err)
			}
		}
	}

	// Remove envoy resources push(CDS,EDS,LDS,RDS)
	return nil
}

// ProxyNeedsPush check if a proxy needs push for this push event.
func ProxyNeedsPush(proxy *Proxy, pushEv *Event) bool {
	if ConfigAffectsProxy(pushEv, proxy) {
		return true
	}

	return false
}

// ConfigAffectsProxy checks if a pushEv will affect a specified proxy. That means whether the push will be performed
// towards the proxy.
func ConfigAffectsProxy(pushEv *Event, proxy *Proxy) bool {
	// Empty changes means "all" to get a backward compatibility.
	if len(pushEv.configsUpdated) == 0 {
		return true
	}

	// No sidecar scope here, so we consider as always-true.
	return true
}

// Tracks connections, increment on each new connection.
var connectionNumber = int64(0)

func connectionID(node string) string {
	id := atomic.AddInt64(&connectionNumber, 1)
	return node + "-" + strconv.FormatInt(id, 10)
}

func versionInfo() string {
	versionMutex.RLock()
	defer versionMutex.RUnlock()
	return version
}
