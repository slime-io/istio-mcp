package mcp

import (
	"fmt"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	gogotypes "github.com/gogo/protobuf/types"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/any"

	istiomcp "istio.io/api/mcp/v1alpha1"

	"istio.io/istio-mcp/pkg/model"
)

func ConfigsToDiscoveryResponse(version string, typeUrl string, configs []model.Config) (*discovery.DiscoveryResponse, error) {
	ret := &discovery.DiscoveryResponse{
		VersionInfo:  version,
		Resources:    nil,
		Canary:       false,
		TypeUrl:      typeUrl,
		Nonce:        "",
		ControlPlane: nil,
	}

	for _, config := range configs {
		// bs, err := proto.Marshal(config.Spec)
		mcpResource, err := ConfigToResource(&config)
		if err != nil {
			return nil, fmt.Errorf("marshal %v met err %v", config.ConfigMeta, err)
		}
		bs, err := proto.Marshal(mcpResource)
		ret.Resources = append(ret.Resources, &any.Any{
			TypeUrl: "type.googleapis.com/istio.mcp.v1alpha1.Resource",
			Value:   bs,
		})
	}

	return ret, nil
}

// Convert from model.Config, which has no associated proto, to MCP Resource proto.
func ConfigToResource(c *model.Config) (*istiomcp.Resource, error) {
	r := &istiomcp.Resource{}

	// MCP, K8S and Istio configs use gogo configs
	// On the wire it's the same as golang proto.
	a, err := gogotypes.MarshalAny(c.Spec)
	if err != nil {
		return nil, err
	}
	r.Body = a
	ts, err := gogotypes.TimestampProto(c.CreationTimestamp)
	if err != nil {
		return nil, err
	}
	r.Metadata = &istiomcp.Metadata{
		Name:        c.Namespace + "/" + c.Name,
		CreateTime:  ts,
		Version:     c.ResourceVersion,
		Labels:      c.Labels,
		Annotations: c.Annotations,
	}

	return r, nil
}
