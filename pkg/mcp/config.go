package mcp

import (
	"fmt"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	mcpv1alpha1 "istio.io/api/mcp/v1alpha1"
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
			return nil, fmt.Errorf("marshal %v met err %v", config.Meta, err)
		}
		bs, err := proto.Marshal(mcpResource)
		ret.Resources = append(ret.Resources, &anypb.Any{
			TypeUrl: "type.googleapis.com/istio.mcp.v1alpha1.Resource",
			Value:   bs,
		})
	}

	return ret, nil
}

// Convert from model.Config, which has no associated proto, to MCP Resource proto.
func ConfigToResource(c *model.Config) (*mcpv1alpha1.Resource, error) {
	r := &mcpv1alpha1.Resource{}

	a, err := model.ToProto(c.Spec)
	if err != nil {
		return nil, err
	}

	r.Body = a
	ts := timestamppb.New(c.CreationTimestamp)
	r.Metadata = &mcpv1alpha1.Metadata{
		Name:        c.Namespace + "/" + c.Name,
		CreateTime:  ts,
		Version:     c.ResourceVersion,
		Labels:      c.Labels,
		Annotations: c.Annotations,
	}

	return r, nil
}
