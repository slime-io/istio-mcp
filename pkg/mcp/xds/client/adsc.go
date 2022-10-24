// Copyright Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package adsc

import (
	"container/list"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"google.golang.org/grpc/credentials"
	"istio.io/istio-mcp/pkg/features"
	"istio.io/pkg/monitoring"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"

	pstruct "github.com/golang/protobuf/ptypes/struct"
	"google.golang.org/grpc"

	"istio.io/api/mesh/v1alpha1"
	"istio.io/istio-mcp/pkg/config/schema/resource"
	mcpclient "istio.io/istio-mcp/pkg/mcp/client"
	istiolog "istio.io/pkg/log"
)

type State int

const (
	StateNew State = iota
	StateConnected
	StateErr
	StateClosed
)

type Change int

func (c Change) String() string {
	switch c {
	case ChangeLen:
		return "add"
	case ChangeDel:
		return "del"
	case ChangeUpdate:
		return "update"
	case ChangeNoUpdate:
		return "noupdate"
	default:
		return "unknown"
	}
}

const (
	ChangeAdd Change = iota
	ChangeDel
	ChangeUpdate
	ChangeNoUpdate
	ChangeLen
)

var (
	typeTag = monitoring.MustCreateLabel("type")
	cfgTag  = monitoring.MustCreateLabel("cfg")

	metricAdscChange = monitoring.NewSum(
		"adsc_change",
		".",
		monitoring.WithLabels(typeTag),
	)

	metricAdscChanges = [ChangeLen]monitoring.Metric{}
)

func init() {
	for i := 0; i < int(ChangeLen); i++ {
		metricAdscChanges[i] = metricAdscChange.With(typeTag.Value(Change(i).String()))
	}
}

// Config for the ADS connection.
type Config struct {
	// Namespace defaults to 'default'
	Namespace string

	// Workload defaults to 'test'
	Workload string

	// Meta includes additional metadata for the node
	Meta *pstruct.Struct

	// NodeType defaults to sidecar. "ingress" and "router" are also supported.
	NodeType string

	// IP is currently the primary key used to locate inbound configs. It is sent by client,
	// must match a known endpoint IP. Tests can use a ServiceEntry to register fake IPs.
	IP string

	// CertDir is the directory where mTLS certs are configured.
	// If CertDir and Secret are empty, an insecure connection will be used.
	// TODO: implement SecretManager for cert dir
	CertDir string

	// Secrets is the interface used for getting keys and rootCA.
	// Secrets security.SecretManager

	// For getting the certificate, using same code as SDS server.
	// Either the JWTPath or the certs must be present.
	JWTPath string

	// XDSSAN is the expected SAN of the XDS server. If not set, the ProxyConfig.DiscoveryAddress is used.
	XDSSAN string

	// InsecureSkipVerify skips client verification the server's certificate chain and host name.
	InsecureSkipVerify bool

	// Watch is a list of resources to watch, represented as URLs (for new XDS resource naming)
	// or type URLs.
	// Watch []string

	// InitialReconnectDelay is the time to wait before attempting to reconnect.
	// If empty reconnect will not be attempted.
	// TODO: client will use exponential backoff to reconnect.
	InitialReconnectDelay time.Duration

	// InitialDiscoveryRequests is a list of resources to watch at first, represented as URLs (for new XDS resource naming)
	// or type URLs.
	InitialDiscoveryRequests []*discovery.DiscoveryRequest

	// backoffPolicy determines the reconnect policy. Based on MCP client.
	BackoffPolicy backoff.BackOff

	// DiscoveryHandler will be called on each DiscoveryResponse.
	// TODO: mirror Generator, allow adding handler per type
	DiscoveryHandler mcpclient.DiscoveryHandler

	// TODO: remove the duplication - all security settings belong here.
	// SecOpts *security.Options

	StateNotifier func(State)
}

// ADSC implements a basic client for ADS, for use in stress tests and tools
// or libraries that need to connect to Istio pilot or other ADS servers.
type ADSC struct {
	// Stream is the GRPC connection stream, allowing direct GRPC send operations.
	// Set after initAdsc is called.
	stream discovery.AggregatedDiscoveryService_StreamAggregatedResourcesClient

	closed      bool
	ackList     *list.List
	ackNotifyCh chan struct{}
	conn        *grpc.ClientConn

	// NodeID is the node identity sent to Pilot.
	nodeID string

	url string

	runTime   time.Time
	watchTime time.Time

	// InitialLoad tracks the time to receive the initial configuration.
	InitialLoad time.Duration

	// httpListeners contains received listeners with a http_connection_manager filter.
	httpListeners map[string]*listener.Listener

	// tcpListeners contains all listeners of type TCP (not-HTTP)
	tcpListeners map[string]*listener.Listener

	// All received clusters of type eds, keyed by name
	edsClusters map[string]*cluster.Cluster

	// All received clusters of no-eds type, keyed by name
	clusters map[string]*cluster.Cluster

	// All received routes, keyed by route name
	routes map[string]*route.RouteConfiguration

	// All received endpoints, keyed by cluster name
	eds map[string]*endpoint.ClusterLoadAssignment

	// Metadata has the node metadata to send to pilot.
	// If nil, the defaults will be used.
	Metadata *pstruct.Struct

	// Updates includes the type of the last update received from the server.
	Updates     chan string
	XDSUpdates  chan *discovery.DiscoveryResponse
	VersionInfo map[string]string

	// Last received message, by type
	Received map[string]*discovery.DiscoveryResponse

	mutex sync.RWMutex

	Mesh *v1alpha1.MeshConfig

	// LocalCacheDir is set to a base name used to save fetched resources.
	// If set, each update will be saved.
	// TODO: also load at startup - so we can support warm up in init-container, and survive
	// restarts.
	LocalCacheDir string

	// RecvWg is for letting goroutines know when the goroutine handling the ADS stream finishes.
	RecvWg sync.WaitGroup

	cfg *Config

	// sendNodeMeta is set to true if the connection is new - and we need to send node meta.,
	sendNodeMeta bool

	sync   map[resource.GroupVersionKind]time.Time
	syncCh chan string

	metricCfgChanges [ChangeLen]map[string]monitoring.Metric
	initReqGvks      []resource.GroupVersionKind
}

var log = istiolog.RegisterScope("xdsc", "xdsc debugging", 0)

// New creates a new ADSC, maintaining a connection to an XDS server.
// Will:
// - get certificate using the Secret provider, if CertRequired
// - connect to the XDS server specified in ProxyConfig
// - send initial request for watched resources
// - wait for respose from XDS server
// - on success, start a background thread to maintain the connection, with exp. backoff.
func New(proxyConfig *v1alpha1.ProxyConfig, opts *Config) (*ADSC, error) {
	// We want to reconnect
	if opts.BackoffPolicy == nil {
		bo := backoff.NewExponentialBackOff()
		bo.MaxElapsedTime = 0
		opts.BackoffPolicy = bo
	}

	adsc, err := initAdsc(proxyConfig.DiscoveryAddress, "", opts)

	return adsc, err
}

// initAdsc connects to a ADS server, with optional MTLS authentication if a cert dir is specified.
func initAdsc(url string, certDir string, opts *Config) (*ADSC, error) {
	if opts == nil {
		opts = &Config{}
	}
	adsc := &ADSC{
		Updates:     make(chan string, 100),
		XDSUpdates:  make(chan *discovery.DiscoveryResponse, 100),
		VersionInfo: map[string]string{},
		url:         url,
		Received:    map[string]*discovery.DiscoveryResponse{},
		RecvWg:      sync.WaitGroup{},
		cfg:         opts,
		syncCh:      make(chan string, len(opts.InitialDiscoveryRequests)),
		sync:        map[resource.GroupVersionKind]time.Time{},
	}
	if certDir != "" {
		opts.CertDir = certDir
	}
	if opts.Namespace == "" {
		opts.Namespace = "default"
	}
	if opts.NodeType == "" {
		opts.NodeType = "sidecar"
	}
	if opts.IP == "" {
		opts.IP = getPrivateIPIfAvailable().String()
	}
	if opts.Workload == "" {
		opts.Workload = "test-1"
	}
	adsc.Metadata = opts.Meta

	adsc.nodeID = fmt.Sprintf("%s~%s~%s.%s~%s.svc.cluster.local", opts.NodeType, opts.IP,
		opts.Workload, opts.Namespace, opts.Namespace)

	// Send the initial requests
	initReqs := opts.InitialDiscoveryRequests
	for _, r := range initReqs {
		adsc.initReqGvks = append(adsc.initReqGvks, resource.TypeUrlToGvk(r.TypeUrl))
	}

	// by default, we assume 1 goroutine decrements the waitgroup (go a.handleRecv()).
	// for synchronizing when the goroutine finishes reading from the gRPC stream.
	adsc.RecvWg.Add(1)

	return adsc, nil
}

// Returns a private IP address, or unspecified IP (0.0.0.0) if no IP is available
func getPrivateIPIfAvailable() net.IP {
	addrs, _ := net.InterfaceAddrs()
	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		default:
			continue
		}
		if !ip.IsLoopback() {
			return ip
		}
	}
	return net.IPv4zero
}

func (a *ADSC) tlsConfig() (*tls.Config, error) {
	var clientCert tls.Certificate
	var serverCABytes []byte
	var err error
	var certName string

	certName = a.cfg.CertDir + "/cert-chain.pem"
	clientCert, err = tls.LoadX509KeyPair(certName,
		a.cfg.CertDir+"/key.pem")
	if err != nil {
		return nil, err
	}
	serverCABytes, err = ioutil.ReadFile(a.cfg.CertDir + "/root-cert.pem")
	if err != nil {
		return nil, err
	}

	serverCAs := x509.NewCertPool()
	if ok := serverCAs.AppendCertsFromPEM(serverCABytes); !ok {
		return nil, err
	}

	// If we supply an expired cert to the server it will just close the connection
	// without useful message.  If the cert is obviously bogus, refuse to use it.
	now := time.Now()
	for _, cert := range clientCert.Certificate {
		cert, err := x509.ParseCertificate(cert)
		if err == nil {
			if now.After(cert.NotAfter) {
				return nil, fmt.Errorf("certificate %s expired %v", certName, cert.NotAfter)
			}
		}
	}

	shost, _, _ := net.SplitHostPort(a.url)
	if a.cfg.XDSSAN != "" {
		shost = a.cfg.XDSSAN
	}
	return &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      serverCAs,
		ServerName:   shost,
		VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			return nil
		},
		InsecureSkipVerify: a.cfg.InsecureSkipVerify,
	}, nil
}

// Close the stream.
func (a *ADSC) Close() {
	a.mutex.Lock()
	prev := a.closed
	if !prev {
		a.closed = true
		if a.conn != nil {
			_ = a.conn.Close()
		}
		close(a.ackNotifyCh)
	}
	a.mutex.Unlock()

	if !prev && a.cfg.StateNotifier != nil {
		a.cfg.StateNotifier(StateClosed)
	}
}

func (a *ADSC) Closed() bool {
	a.mutex.RLock()
	defer a.mutex.RUnlock()
	return a.closed
}

// Run will start the ADS client.
func (a *ADSC) Run() error {
	a.reconnect()
	return nil
}

// run will run one connection to the ADS client.
func (a *ADSC) run() error {
	var (
		err  error
		conn *grpc.ClientConn
	)

	a.runTime = time.Now()

	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(features.MaxRecvMsgSize)),
	}
	if len(a.cfg.CertDir) > 0 {
		tlsCfg, err := a.tlsConfig()
		if err != nil {
			return err
		}
		creds := credentials.NewTLS(tlsCfg)

		opts = append(opts, grpc.WithTransportCredentials(creds))
		conn, err = grpc.Dial(a.url, opts...)
		if err != nil {
			return err
		}
	} else {
		opts = append(opts, grpc.WithInsecure())
		conn, err = grpc.Dial(a.url, opts...)
		if err != nil {
			return err
		}
	}

	var (
		prevConn        *grpc.ClientConn
		prevAckNotifyCh chan struct{}
	)
	a.mutex.Lock()
	prevConn, a.conn = a.conn, conn
	prevAckNotifyCh, a.ackNotifyCh = a.ackNotifyCh, make(chan struct{}, 1)
	a.ackList = list.New()
	a.mutex.Unlock()
	if prevConn != nil {
		_ = prevConn.Close()
	}
	if prevAckNotifyCh != nil {
		close(prevAckNotifyCh)
	}

	xds := discovery.NewAggregatedDiscoveryServiceClient(a.conn)
	edsstr, err := xds.StreamAggregatedResources(context.Background())
	if err != nil {
		return err
	}
	a.stream = edsstr
	a.sendNodeMeta = true

	for _, r := range a.cfg.InitialDiscoveryRequests {
		if r.TypeUrl == V3ClusterType {
			a.watchTime = time.Now()
		}
		_ = a.Send(r)
	}

	if a.cfg.StateNotifier != nil {
		a.cfg.StateNotifier(StateConnected)
	}

	go a.ackTask(a.ackNotifyCh, a.ackList)
	go a.handleRecv()
	return nil
}

// HasSynced returns true if MCP configs have synced
func (a *ADSC) HasSynced() bool {
	for _, gvk := range a.initReqGvks {
		a.mutex.RLock()
		t := a.sync[gvk]
		a.mutex.RUnlock()
		if t.IsZero() {
			log.Warnf("NOT SYNCE %v", gvk)
			return false
		}
	}
	return true
}

func (a *ADSC) reconnect() {
	if a.Closed() {
		return
	}

	err := a.run()
	if err == nil {
		log.Infof("connect/reconnect succeeds, reset backoffpolicy")
		a.cfg.BackoffPolicy.Reset()
	} else {
		waitTime := a.cfg.BackoffPolicy.NextBackOff()
		log.Infof("connect/reconnect fails, backoff for %v", waitTime)
		time.AfterFunc(waitTime, a.reconnect)
	}
}

func (a *ADSC) handleRecv() {
	for {
		var err error
		msg, err := a.stream.Recv()
		if err != nil {
			log.Infof("Connection for node %v with err: %v", a.nodeID, err)
			closed := a.Closed()

			if !closed {
				if a.cfg.StateNotifier != nil {
					a.cfg.StateNotifier(StateErr)
				}

				// if 'reconnect' enabled - schedule a new Run
				if a.cfg.BackoffPolicy != nil {
					time.AfterFunc(a.cfg.BackoffPolicy.NextBackOff(), a.reconnect)
					return
				}
			}

			// closed or no-reconnect
			a.RecvWg.Done()
			if !closed {
				a.Close()
			}
			a.WaitClear()
			a.Updates <- ""
			a.XDSUpdates <- nil
			return
		}

		// Group-value-kind - used for high level api generator.
		gvk := strings.SplitN(msg.TypeUrl, "/", 3)

		log.Infof("Received %s type %s cnt=%d nonce=%s", a.url, msg.TypeUrl, len(msg.Resources), msg.Nonce)
		if a.cfg.DiscoveryHandler != nil {
			if err := a.cfg.DiscoveryHandler.HandleResponse(msg); err != nil {
				log.Errorf("handle msg %s met err %v", msg.TypeUrl, err)
			}
		}

		a.mutex.Lock()
		if len(gvk) == 3 { // istio types
			gt := resource.GroupVersionKind{Group: gvk[0], Version: gvk[1], Kind: gvk[2]}
			a.sync[gt] = time.Now()
		}
		// a.Received[msg.TypeUrl] = msg
		a.mutex.Unlock()
		a.ack(msg)

		select {
		case a.XDSUpdates <- msg:
		default:
		}
	}
}

func (a *ADSC) node() *core.Node {
	n := &core.Node{
		Id: a.nodeID,
	}
	if a.Metadata == nil {
		n.Metadata = &pstruct.Struct{
			Fields: map[string]*pstruct.Value{
				"ISTIO_VERSION": {Kind: &pstruct.Value_StringValue{StringValue: "65536.65536.65536"}},
			},
		}
	} else {
		n.Metadata = a.Metadata
		if a.Metadata.Fields["ISTIO_VERSION"] == nil {
			a.Metadata.Fields["ISTIO_VERSION"] = &pstruct.Value{Kind: &pstruct.Value_StringValue{StringValue: "65536.65536.65536"}}
		}
	}
	return n
}

// Send Raw send of a request.
func (a *ADSC) Send(req *discovery.DiscoveryRequest) error {
	if a.sendNodeMeta {
		req.Node = a.node()
		a.sendNodeMeta = false
	}
	return a.stream.Send(req)
}

// WaitClear will clear the waiting events, so next call to Wait will get
// the next push type.
func (a *ADSC) WaitClear() {
	for {
		select {
		case <-a.Updates:
		default:
			return
		}
	}
}

// EndpointsJSON returns the endpoints, formatted as JSON, for debugging.
func (a *ADSC) EndpointsJSON() string {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	out, _ := json.MarshalIndent(a.eds, " ", " ")
	return string(out)
}

//func ConfigInitialRequests() []*discovery.DiscoveryRequest {
//	out := make([]*discovery.DiscoveryRequest, 0, len(collections.Pilot.All())+1)
//	out = append(out, &discovery.DiscoveryRequest{
//		TypeUrl: collections.IstioMeshV1Alpha1MeshConfig.Resource().GroupVersionKind().String(),
//	})
//	for _, sch := range collections.Pilot.All() {
//		out = append(out, &discovery.DiscoveryRequest{
//			TypeUrl: sch.Resource().GroupVersionKind().String(),
//		})
//	}
//
//	return out
//}

// WatchConfig will use the new experimental API watching, similar with MCP.
//func (a *ADSC) WatchConfig() {
//	for _, rsc := range a.cfg.Watch {
//		_ = a.stream.Send(&discovery.DiscoveryRequest{
//			ResponseNonce: time.Now().String(),
//			Node:          a.node(),
//			TypeUrl:       rsc,
//		})
//	}
//}

// WaitConfigSync will wait for the memory controller to sync.
func (a *ADSC) WaitConfigSync(max time.Duration) bool {
	// TODO: when adding support for multiple config controllers (matching MCP), make sure the
	// new stores support reporting sync events on the syncCh, to avoid the sleep loop from MCP.
	if a.HasSynced() {
		return true
	}
	maxCh := time.After(max)
	for {
		select {
		case <-a.syncCh:
			if a.HasSynced() {
				return true
			}
		case <-maxCh:
			return a.HasSynced()
		}
	}
}

//func (a *ADSC) sendRsc(typeurl string, rsc []string) {
//	ex := a.Received[typeurl]
//	version := ""
//	nonce := ""
//	if ex != nil {
//		version = ex.VersionInfo
//		nonce = ex.Nonce
//	}
//	_ = a.stream.Send(&discovery.DiscoveryRequest{
//		ResponseNonce: nonce,
//		VersionInfo:   version,
//		Node:          a.node(),
//		TypeUrl:       typeurl,
//		ResourceNames: rsc,
//	})
//}

type ackItem struct {
	id     string
	stream discovery.AggregatedDiscoveryService_StreamAggregatedResourcesClient
	ack    *discovery.DiscoveryRequest
}

func (a *ADSC) ackTask(ackNotifyCh chan struct{}, ackList *list.List) {
	for {
		select {
		case _, ok := <-ackNotifyCh:
			if !ok {
				return
			}
		}

		for {
			a.mutex.Lock()
			f := ackList.Front()
			if f == nil {
				break
			}
			ackList.Remove(f)
			a.mutex.Unlock()

			item := f.Value.(ackItem)
			if err := item.stream.Send(item.ack); err != nil {
				log.Errorf("send ack %s %s %s met err %v", item.id, item.ack.TypeUrl, item.ack.VersionInfo, err)
			}
		}
	}
}

func (a *ADSC) ack(msg *discovery.DiscoveryResponse) {
	if msg.Nonce == "" {
		return // do not ack for NO-DATA push
	}

	stype := GetShortType(msg.TypeUrl)
	var resources []string
	// TODO: Send routes also in future.
	if stype == EndpointShortType {
		for c := range a.edsClusters {
			resources = append(resources, c)
		}
	}

	ack := &discovery.DiscoveryRequest{
		ResponseNonce: msg.Nonce,
		TypeUrl:       msg.TypeUrl,
		Node:          a.node(),
		VersionInfo:   msg.VersionInfo,
		ResourceNames: resources,
	}

	if a.closed {
		a.mutex.Unlock()
		return
	}
	a.mutex.Lock()
	a.ackList.PushBack(ackItem{
		id:     msg.Nonce,
		stream: a.stream,
		ack:    ack,
	})

	select {
	case a.ackNotifyCh <- struct{}{}:
	default:
	}
	a.mutex.Unlock()
}

// GetHTTPListeners returns all the http listeners.
func (a *ADSC) GetHTTPListeners() map[string]*listener.Listener {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.httpListeners
}

// GetTCPListeners returns all the tcp listeners.
func (a *ADSC) GetTCPListeners() map[string]*listener.Listener {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.tcpListeners
}

// GetEdsClusters returns all the eds type clusters.
func (a *ADSC) GetEdsClusters() map[string]*cluster.Cluster {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.edsClusters
}

// GetClusters returns all the non-eds type clusters.
func (a *ADSC) GetClusters() map[string]*cluster.Cluster {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.clusters
}

// GetRoutes returns all the routes.
func (a *ADSC) GetRoutes() map[string]*route.RouteConfiguration {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.routes
}

// GetEndpoints returns all the routes.
func (a *ADSC) GetEndpoints() map[string]*endpoint.ClusterLoadAssignment {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.eds
}
