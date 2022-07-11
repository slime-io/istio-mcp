package http

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	//"sigs.k8s.io/yaml"

	"gopkg.in/yaml.v2"
	"istio.io/pkg/log"

	"istio.io/istio-mcp/pkg/config/schema/resource"
	"istio.io/istio-mcp/pkg/mcp"
	"istio.io/istio-mcp/pkg/model"
)

const (
	DefaultLongPollingInterval = 60 * time.Second
	DefaultHttpListenAddr      = "0.0.0.0"
	DefaultHttpListenPort      = 8432
)

var DefaultHttpServerOptions = ServerOptions{
	LongPollingInterval: DefaultLongPollingInterval,
	Addr:                DefaultHttpListenAddr,
	Port:                DefaultHttpListenPort,
}

func init() {
	if v := os.Getenv("MCP_HTTP_URL"); v != "" {
		if o, err := ParseServerOptions(v); err != nil {
			fmt.Printf("invalid mcp http url %s, skip", v)
		} else if o != nil {
			DefaultHttpServerOptions = *o
			fmt.Printf("DefaultHttpServerOptions set to %s\n", v)
		}
	}
}

func DefaultServerOptions() *ServerOptions {
	o := DefaultHttpServerOptions
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

	if v := u.Query().Get("pollInterval"); v != "" {
		if o.LongPollingInterval, err = time.ParseDuration(v); err != nil {
			return err
		}
	}

	if q := u.Query(); q != nil {
		if v := q["resource"]; v != nil {
			var resources []resource.GroupVersionKind
			for _, typeUrl := range v {
				rsc := resource.TypeUrlToGvk(typeUrl)
				if rsc == resource.AllGvk {
					resources = []resource.GroupVersionKind{rsc}
					break
				} else {
					resources = append(resources, rsc)
				}
			}
			o.Resources = resources
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

type ServerExtraOptions struct {
	ServeMuxHandler func(mux *http.ServeMux)
}

type ServerOptions struct {
	LongPollingInterval time.Duration
	Addr                string
	Port                int
	Resources           []resource.GroupVersionKind

	ServerExtraOptions
}

func (o *ServerOptions) ApplyExtra(extra ServerExtraOptions) {
	o.ServerExtraOptions = extra
}

func NewServer(options *ServerOptions) *httpServer {
	ret := &httpServer{
		conns:   map[uint64]*httpConn{},
		options: options,
	}
	srvMux := http.NewServeMux()
	srvMux.HandleFunc("/configs", ret.handleConfigs)
	if options.ServeMuxHandler != nil {
		options.ServeMuxHandler(srvMux)
	}
	ret.srvMux = srvMux

	return ret
}

var connId uint64

type httpConn struct {
	clientId   string
	connId     uint64
	clientAddr string
	pushCh     chan struct{}

	svr *httpServer
}

func (conn *httpConn) clientInfo() mcp.ClientInfo {
	return mcp.ClientInfo{
		ClientId: conn.clientId,
		ConnId:   strconv.FormatUint(conn.connId, 10),
		Addr:     conn.clientAddr,
	}
}

func (conn *httpConn) notifyPush() bool {
	select {
	case conn.pushCh <- struct{}{}:
		return true
	default:
		return false
	}
}

func newHttpConn(svr *httpServer) *httpConn {
	return &httpConn{
		connId: atomic.AddUint64(&connId, 1),
		pushCh: make(chan struct{}, 1),

		svr: svr,
	}
}

type HttpServer interface {
	mcp.Server
}

type httpServer struct {
	store model.ConfigStore
	conns map[uint64]*httpConn
	mux   sync.Mutex

	srvMux  *http.ServeMux
	options *ServerOptions

	clientEventHandlers []func(event mcp.ClientEvent)
}

var _ HttpServer = &httpServer{}

func (hs *httpServer) RegisterClientEventHandler(h func(mcp.ClientEvent)) {
	hs.clientEventHandlers = append(hs.clientEventHandlers, h)
}

func (hs *httpServer) SetConfigStore(store model.ConfigStore) {
	hs.store = store
}

func (hs *httpServer) NotifyPush() {
	hs.mux.Lock()
	defer hs.mux.Unlock()
	for _, conn := range hs.conns {
		conn.notifyPush()
	}
}

func (hs *httpServer) Start(ctx context.Context) {
	go func() {
		if err := http.ListenAndServe(fmt.Sprintf("%s:%d", hs.options.Addr, hs.options.Port), hs.srvMux); err != nil {
			log.Errorf("http server return err: %v", err)
		}
	}()
}

func (hs *httpServer) handleConfigs(writer http.ResponseWriter, request *http.Request) {
	timeoutCh := time.After(hs.options.LongPollingInterval)
	if err := request.ParseForm(); err != nil {
		http.Error(writer, "invalid form", http.StatusBadRequest)
		return
	}

	reqVer := request.URL.Query().Get("version")
	typeUrl := request.URL.Query().Get("typeUrl")
	serialize := request.URL.Query().Get("ser")
	ns := request.URL.Query().Get("ns")
	clientId := request.URL.Query().Get("clientId")
	if clientId == "" {
		clientId = request.RemoteAddr
	}
	if ns == "" {
		ns = resource.AllNamespace
	}

	gvk := resource.TypeUrlToGvk(typeUrl)
	if resource.IsK8sShortGvk(gvk) {
		gvk = resource.ConvertShortK8sGvk(gvk)
	}

	conn := newHttpConn(hs)
	conn.clientAddr = request.RemoteAddr
	hs.addConn(conn)
	defer hs.removeConn(conn.connId)
	conn.notifyPush()

	var (
		configs []model.Config
		timeout bool
		ver     = reqVer
		changed bool
	)

	for {
		select {
		case <-conn.pushCh:
		case <-timeoutCh:
			timeout = true
		}

		snap := hs.store.Snapshot(ns)
		if snap == nil {
			changed = false
		} else if curVer := snap.Version(); curVer == model.VersionNotInitialized || curVer == reqVer { // Not initialized or not updated.
			changed = false
		} else if lastSnap := hs.store.VersionSnapshot(reqVer, ns); lastSnap != nil &&
			len(lastSnap.Configs(resource.AllGvk, resource.AllNamespace, "")) == 0 &&
			len(snap.Configs(resource.AllGvk, resource.AllNamespace, "")) == 0 {
			changed = false // both empty
		} else {
			changed = true
			ver = curVer
			configs = snap.Configs(gvk, resource.AllNamespace, "")
		}

		// no need to push content
		if changed || timeout {
			break
		}
		// if timeout we should at least return a same-version & empty-content response.
	}

	var bs []byte
	var err error

	if serialize == "yaml" {
		bs, err = yaml.Marshal(configs)
		if err != nil {
			log.Errorf("yaml marshal config of type %s and version %s met err %v", typeUrl, ver, err)
			http.Error(writer, "", http.StatusInternalServerError)
			return
		}
	} else {
		disResp, err := mcp.ConfigsToDiscoveryResponse(ver, typeUrl, configs)
		if err == nil {
			bs, err = proto.Marshal(disResp)
		}
		if err != nil {
			log.Errorf("configsToDiscoveryResponse for %s %s met err %v", typeUrl, ver, err)
			http.Error(writer, "", http.StatusInternalServerError)
			return
		}
	}

	writer.Header().Add("version", ver)
	// writer.WriteHeader(http.StatusOK)

	if bs != nil {
		if _, err := writer.Write(bs); err != nil {
			log.Errorf("write to stream of conn %d met err %v", connId, err)
			return
		}
	}

	if changed {
		log.Infof("client %s type %s update to version %s with configs %d", request.RemoteAddr, typeUrl, ver, len(configs))
	}
}

func (hs *httpServer) addConn(conn *httpConn) {
	log.Debugf("add conn %s-%s-%d", conn.clientId, conn.clientAddr, conn.connId)
	hs.mux.Lock()
	hs.conns[conn.connId] = conn
	hs.mux.Unlock()

	// consider client events from a single connId is sequential.
	if len(hs.clientEventHandlers) > 0 {
		ev := mcp.ClientEvent{
			Clients:   []mcp.ClientInfo{conn.clientInfo()},
			EventType: mcp.ClientEventAdd,
		}

		for _, h := range hs.clientEventHandlers {
			h(ev)
		}
	}
}

func (hs *httpServer) removeConn(connId uint64) *httpConn {
	var conn *httpConn

	hs.mux.Lock()
	defer func() {
		hs.mux.Unlock()
		if conn != nil {
			log.Debugf("remove conn %s-%s-%d", conn.clientId, conn.clientAddr, connId)

			if len(hs.clientEventHandlers) > 0 {
				ev := mcp.ClientEvent{
					Clients:   []mcp.ClientInfo{conn.clientInfo()},
					EventType: mcp.ClientEventDelete,
				}

				for _, h := range hs.clientEventHandlers {
					h(ev)
				}
			}
		}
	}()

	ret, ok := hs.conns[connId]
	if ok {
		conn = ret
		delete(hs.conns, connId)
	}
	return ret
}
