package xds

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"
	"istio.io/istio-mcp/pkg/features"
	"istio.io/pkg/log"

	structpb "github.com/golang/protobuf/ptypes/struct"
	// gogojsonpb "github.com/gogo/protobuf/jsonpb"
	"github.com/golang/protobuf/jsonpb"

	"istio.io/istio-mcp/pkg/model"
)

const (
	serviceNodeSeparator = "~"
)

// StringList is a list that will be marshaled to a comma separate string in Json
type StringList []string

func (l StringList) MarshalJSON() ([]byte, error) {
	if l == nil {
		return nil, nil
	}
	return []byte(`"` + strings.Join(l, ",") + `"`), nil
}

func (l *StringList) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || string(data) == `""` {
		*l = []string{}
	} else {
		*l = strings.Split(string(data[1:len(data)-1]), ",")
	}
	return nil
}

// Resources is an alias for array of marshaled resources.
type Resources struct {
	Data    []*any.Any
	Version string
}

// XdsUpdates include information about the subset of updated resources.
// See for example EDS incremental updates.
type XdsUpdates = map[model.ConfigKey]struct{}

// XdsResourceGenerator creates the response for a typeURL DiscoveryRequest. If no generator is associated
// with a Proxy, the default (a networking.core.ConfigGenerator instance) will be used.
// The server may associate a different generator based on client metadata. Different
// WatchedResources may use same or different Generator.
type XdsResourceGenerator interface {
	Generate(proxy *Proxy, push *PushContext, w *WatchedResource, updates XdsUpdates) Resources
}

// WatchedResource tracks an active DiscoveryRequest subscription.
type WatchedResource struct {
	// TypeUrl is copied from the DiscoveryRequest.TypeUrl that initiated watching this resource.
	// nolint
	TypeUrl string

	// ResourceNames tracks the list of resources that are actively watched. If empty, all resources of the
	// TypeUrl type are watched.
	// For endpoints the resource names will have list of clusters and for clusters it is empty.
	ResourceNames []string
	resourceSent  map[uint32]struct{}

	// VersionSent is the version of the resource included in the last sent response.
	// It corresponds to the [Cluster/Route/Listener]VersionSent in the XDS package.
	VersionSent string

	// NonceSent is the nonce sent in the last sent response. If it is equal with NonceAcked, the
	// last message has been processed. If empty: we never sent a message of this type.
	NonceSent        string
	NonceSentRecords []string

	// VersionAcked represents the version that was applied successfully. It can be different from
	// VersionSent: if NonceSent == NonceAcked and versions are different it means the client rejected
	// the last version, and VersionAcked is the last accepted and active config.
	// If empty it means the client has no accepted/valid version, and is not ready.
	VersionAcked string

	// NonceAcked is the last acked message.
	NonceAcked string

	// LastSent tracks the time of the generated push, to determine the time it takes the client to ack.
	LastSent time.Time

	// Updates count the number of generated updates for the resource
	Updates int

	// LastSize tracks the size of the last update
	LastSize int

	// Last request contains the last DiscoveryRequest received for
	// this type. Generators are called immediately after each request,
	// and may use the information in DiscoveryRequest.
	// Note that Envoy may send multiple requests for the same type, for
	// example to update the set of watched resources or to ACK/NACK.
	LastRequest *discovery.DiscoveryRequest
}

func NewWatchedResource(typeUrl string) *WatchedResource {
	ret := &WatchedResource{
		TypeUrl:          typeUrl,
		NonceSentRecords: make([]string, features.SentNonceRecordNum),
		resourceSent:     map[uint32]struct{}{},
	}
	return ret
}

func (w *WatchedResource) IsRecorded(nonce string) bool {
	if w.NonceSent == nonce {
		return true
	}
	for _, v := range w.NonceSentRecords {
		if v != "" && v == nonce {
			return true
		}
	}

	return false
}

func (w *WatchedResource) RecordNonceSent(nonceSent string) {
	w.NonceSent = nonceSent
	for idx, v := range w.NonceSentRecords {
		if v == "" {
			w.NonceSentRecords[idx] = nonceSent
			w.NonceSentRecords[(idx+1)%len(w.NonceSentRecords)] = ""
			break
		}
	}
}

func (w *WatchedResource) RecordResourceSent(key model.ConfigKey) {
	w.resourceSent[key.HashCode()] = struct{}{}
}

func (w *WatchedResource) IsResourceSent(key model.ConfigKey) bool {
	_, ok := w.resourceSent[key.HashCode()]
	return ok
}

// NodeMetadata defines the metadata associated with a proxy
// Fields should not be assumed to exist on the proxy, especially newly added fields which will not exist
// on older versions.
// The JSON field names should never change, as they are needed for backward compatibility with older proxies
// nolint: maligned
type NodeMetadata struct {
	// IstioVersion specifies the Istio version associated with the proxy
	IstioVersion string `json:"ISTIO_VERSION,omitempty"`

	// IstioRevision specifies the Istio revision associated with the proxy.
	// Mostly used when istiod requests the upstream.
	IstioRevision string `json:"ISTIO_REVISION,omitempty"`

	// Labels specifies the set of workload instance (ex: k8s pod) labels associated with this node.
	Labels map[string]string `json:"LABELS,omitempty"`

	// InstanceIPs is the set of IPs attached to this proxy
	InstanceIPs StringList `json:"INSTANCE_IPS,omitempty"`

	// Namespace is the namespace in which the workload instance is running.
	Namespace string `json:"NAMESPACE,omitempty"`

	// ServiceAccount specifies the service account which is running the workload.
	ServiceAccount string `json:"SERVICE_ACCOUNT,omitempty"`

	// RouterMode indicates whether the proxy is functioning as a SNI-DNAT router
	// processing the AUTO_PASSTHROUGH gateway servers
	RouterMode string `json:"ROUTER_MODE,omitempty"`

	// MeshID specifies the mesh ID environment variable.
	MeshID string `json:"MESH_ID,omitempty"`

	// ClusterID defines the cluster the node belongs to.
	ClusterID string `json:"CLUSTER_ID,omitempty"`

	// Network defines the network the node belongs to. It is an optional metadata,
	// set at injection time. When set, the Endpoints returned to a note and not on same network
	// will be replaced with the gateway defined in the settings.
	Network string `json:"NETWORK,omitempty"`

	// RequestedNetworkView specifies the networks that the proxy wants to see
	RequestedNetworkView StringList `json:"REQUESTED_NETWORK_VIEW,omitempty"`

	PolicyCheck                  string `json:"policy.istio.io/check,omitempty"`
	PolicyCheckRetries           string `json:"policy.istio.io/checkRetries,omitempty"`
	PolicyCheckBaseRetryWaitTime string `json:"policy.istio.io/checkBaseRetryWaitTime,omitempty"`
	PolicyCheckMaxRetryWaitTime  string `json:"policy.istio.io/checkMaxRetryWaitTime,omitempty"`

	// TLSServerCertChain is the absolute path to server cert-chain file
	TLSServerCertChain string `json:"TLS_SERVER_CERT_CHAIN,omitempty"`
	// TLSServerKey is the absolute path to server private key file
	TLSServerKey string `json:"TLS_SERVER_KEY,omitempty"`
	// TLSServerRootCert is the absolute path to server root cert file
	TLSServerRootCert string `json:"TLS_SERVER_ROOT_CERT,omitempty"`
	// TLSClientCertChain is the absolute path to client cert-chain file
	TLSClientCertChain string `json:"TLS_CLIENT_CERT_CHAIN,omitempty"`
	// TLSClientKey is the absolute path to client private key file
	TLSClientKey string `json:"TLS_CLIENT_KEY,omitempty"`
	// TLSClientRootCert is the absolute path to client root cert file
	TLSClientRootCert string `json:"TLS_CLIENT_ROOT_CERT,omitempty"`

	CertBaseDir string `json:"BASE,omitempty"`

	// StsPort specifies the port of security token exchange server (STS).
	// Used by envoy filters
	StsPort string `json:"STS_PORT,omitempty"`

	// IdleTimeout specifies the idle timeout for the proxy, in duration format (10s).
	// If not set, no timeout is set.
	IdleTimeout string `json:"IDLE_TIMEOUT,omitempty"`

	// HTTP10 indicates the application behind the sidecar is making outbound http requests with HTTP/1.0
	// protocol. It will enable the "AcceptHttp_10" option on the http options for outbound HTTP listeners.
	// Alpha in 1.1, based on feedback may be turned into an API or change. Set to "1" to enable.
	HTTP10 string `json:"HTTP10,omitempty"`

	// Generator indicates the client wants to use a custom Generator plugin.
	Generator string `json:"GENERATOR,omitempty"`

	// DNSCapture indicates whether the workload has enabled dns capture
	DNSCapture string `json:"DNS_CAPTURE,omitempty"`

	// Contains a copy of the raw metadata. This is needed to lookup arbitrary values.
	// If a value is known ahead of time it should be added to the struct rather than reading from here,
	Raw map[string]interface{} `json:"-"`
}

// BootstrapNodeMetadata is a superset of NodeMetadata, intended to model the entirety of the node metadata
// we configure in the Envoy bootstrap. This is split out from NodeMetadata to explicitly segment the parameters
// that are consumed by Pilot from the parameters used only as part of the bootstrap. Fields used by bootstrap only
// are consumed by Envoy itself, such as the telemetry filters.
type BootstrapNodeMetadata struct {
	NodeMetadata

	// ExchangeKeys specifies a list of metadata keys that should be used for Node Metadata Exchange.
	ExchangeKeys StringList `json:"EXCHANGE_KEYS,omitempty"`

	// InstanceName is the short name for the workload instance (ex: pod name)
	// replaces POD_NAME
	InstanceName string `json:"NAME,omitempty"`

	// WorkloadName specifies the name of the workload represented by this node.
	WorkloadName string `json:"WORKLOAD_NAME,omitempty"`

	// Owner specifies the workload owner (opaque string). Typically, this is the owning controller of
	// of the workload instance (ex: k8s deployment for a k8s pod).
	Owner string `json:"OWNER,omitempty"`

	// PlatformMetadata contains any platform specific metadata
	PlatformMetadata map[string]string `json:"PLATFORM_METADATA,omitempty"`

	StatsInclusionPrefixes string `json:"sidecar.istio.io/statsInclusionPrefixes,omitempty"`
	StatsInclusionRegexps  string `json:"sidecar.istio.io/statsInclusionRegexps,omitempty"`
	StatsInclusionSuffixes string `json:"sidecar.istio.io/statsInclusionSuffixes,omitempty"`
	ExtraStatTags          string `json:"sidecar.istio.io/extraStatTags,omitempty"`
}

type Proxy struct {
	Metadata *NodeMetadata

	// Type specifies the node type. First part of the ID.
	Type NodeType

	// IPAddresses is the IP addresses of the proxy used to identify it and its
	// co-located service instances. Example: "10.60.1.6". In some cases, the host
	// where the poxy and service instances reside may have more than one IP address
	IPAddresses []string

	// ID is the unique platform-specific sidecar proxy ID. For k8s it is the pod ID and
	// namespace.
	ID string

	// DNSDomain defines the DNS domain suffix for short hostnames (e.g.
	// "default.svc.cluster.local")
	DNSDomain string

	// Indicates wheteher proxy supports IPv6 addresses
	ipv6Support bool

	// Indicates wheteher proxy supports IPv4 addresses
	ipv4Support bool

	// GlobalUnicastIP stores the globacl unicast IP if available, otherwise nil
	GlobalUnicastIP string

	// XdsResourceGenerator is used to generate resources for the node, based on the PushContext.
	// If nil, the default networking/core v2 generator is used. This field can be set
	// at connect time, based on node metadata, to trigger generation of a different style
	// of configuration.
	XdsResourceGenerator XdsResourceGenerator

	// Active contains the list of watched resources for the proxy, keyed by the DiscoveryRequest short type.
	Active map[string]*WatchedResource

	// ActiveExperimental contains the list of watched resources for the proxy, keyed by the canonical DiscoveryRequest type.
	// Note that the key may not be equal to the proper TypeUrl. For example, Envoy types like Cluster will share a single
	// key for multiple versions.
	ActiveExperimental map[string]*WatchedResource
}

// DiscoverIPVersions discovers the IP Versions supported by Proxy based on its IP addresses.
func (node *Proxy) DiscoverIPVersions() {
	for i := 0; i < len(node.IPAddresses); i++ {
		addr := net.ParseIP(node.IPAddresses[i])
		if addr == nil {
			// Should not happen, invalid IP in proxy's IPAddresses slice should have been caught earlier,
			// skip it to prevent a panic.
			continue
		}
		if addr.IsGlobalUnicast() {
			node.GlobalUnicastIP = addr.String()
		}
		if addr.To4() != nil {
			node.ipv4Support = true
		} else {
			node.ipv6Support = true
		}
	}
}

// MessageToAny converts from proto message to proto Any
func MessageToAny(msg proto.Message) *any.Any {
	out, err := MessageToAnyWithError(msg)
	if err != nil {
		log.Error(fmt.Sprintf("error marshaling Any %s: %v", msg.String(), err))
		return nil
	}
	return out
}

// MessageToAnyWithError converts from proto message to proto Any
func MessageToAnyWithError(msg proto.Message) (*any.Any, error) {
	b := proto.NewBuffer(nil)
	b.SetDeterministic(true)
	err := b.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return &any.Any{
		// nolint: staticcheck
		TypeUrl: "type.googleapis.com/" + proto.MessageName(msg),
		Value:   b.Bytes(),
	}, nil
}

// listEqualUnordered checks that two lists contain all the same elements
func listEqualUnordered(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	first := make(map[string]struct{}, len(a))
	for _, c := range a {
		first[c] = struct{}{}
	}
	for _, c := range b {
		_, f := first[c]
		if !f {
			return false
		}
	}
	return true
}

func GetShortType(typeURL string) string {
	switch typeURL {
	// case v2.ClusterType, ClusterType:
	//	return ClusterShortType
	// case v2.ListenerType, ListenerType:
	//	return ListenerShortType
	// case v2.RouteType, RouteType:
	//	return RouteShortType
	// case v2.EndpointType, EndpointType:
	//	return EndpointShortType
	default:
		return typeURL
	}
}

// ParseMetadata parses the opaque Metadata from an Envoy Node into string key-value pairs.
// Any non-string values are ignored.
func ParseMetadata(metadata *structpb.Struct) (*NodeMetadata, error) {
	if metadata == nil {
		return &NodeMetadata{}, nil
	}

	buf := &bytes.Buffer{}
	if err := (&jsonpb.Marshaler{OrigName: true}).Marshal(buf, metadata); err != nil {
		return nil, fmt.Errorf("failed to read node metadata %v: %v", metadata, err)
	}
	meta := &BootstrapNodeMetadata{}
	if err := json.Unmarshal(buf.Bytes(), meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal node metadata (%v): %v", buf.String(), err)
	}
	return &meta.NodeMetadata, nil
}

// ParseServiceNodeWithMetadata parse the Envoy Node from the string generated by ServiceNode
// function and the metadata.
func ParseServiceNodeWithMetadata(s string, metadata *NodeMetadata) (*Proxy, error) {
	parts := strings.Split(s, serviceNodeSeparator)
	out := &Proxy{
		Metadata: metadata,
	}

	if len(parts) != 4 {
		return out, fmt.Errorf("missing parts in the service node %q", s)
	}

	if !IsApplicationNodeType(NodeType(parts[0])) {
		return out, fmt.Errorf("invalid node type (valid types: sidecar, router in the service node %q", s)
	}
	out.Type = NodeType(parts[0])

	// Get all IP Addresses from Metadata
	if hasValidIPAddresses(metadata.InstanceIPs) {
		out.IPAddresses = metadata.InstanceIPs
	} else if isValidIPAddress(parts[1]) {
		// Fall back, use IP from node id, it's only for backward-compatibility, IP should come from metadata
		out.IPAddresses = append(out.IPAddresses, parts[1])
	}

	// Does query from ingress or router have to carry valid IP address?
	if len(out.IPAddresses) == 0 {
		return out, fmt.Errorf("no valid IP address in the service node id or metadata")
	}

	out.ID = parts[2]
	out.DNSDomain = parts[3]
	if len(metadata.IstioVersion) == 0 {
		log.Warnf("Istio Version is not found in metadata for %v, which may have undesirable side effects", out.ID)
	}
	return out, nil
}

// NodeType decides the responsibility of the proxy serves in the mesh
type NodeType string

const (
	// SidecarProxy type is used for sidecar proxies in the application containers
	SidecarProxy NodeType = "sidecar"

	// Router type is used for standalone proxies acting as L7/L4 routers
	Router NodeType = "router"
)

// IsApplicationNodeType verifies that the NodeType is one of the declared constants in the model
func IsApplicationNodeType(nType NodeType) bool {
	switch nType {
	case SidecarProxy, Router:
		return true
	default:
		return false
	}
}

// hasValidIPAddresses returns true if the input ips are all valid, otherwise returns false.
func hasValidIPAddresses(ipAddresses []string) bool {
	if len(ipAddresses) == 0 {
		return false
	}
	for _, ipAddress := range ipAddresses {
		if !isValidIPAddress(ipAddress) {
			return false
		}
	}
	return true
}

// Tell whether the given IP address is valid or not
func isValidIPAddress(ip string) bool {
	return net.ParseIP(ip) != nil
}

// PushContext tracks the status of a push - metrics and errors.
// Metrics are reset after a push - at the beginning all
// values are zero, and when push completes the status is reset.
// The struct is exposed in a debug endpoint - fields public to allow
// easy serialization as json.
type PushContext struct {
	Version string

	// Config interface for listing routing rules
	model.ConfigStore `json:"-"`
}
