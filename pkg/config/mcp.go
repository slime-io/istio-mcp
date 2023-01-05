package config

import (
	"fmt"
	"strings"

	"github.com/gogo/protobuf/types"
	mcp "istio.io/api/mcp/v1alpha1"

	"istio.io/istio-mcp/pkg/model"
)

func McpToPilot(rev string, m *mcp.Resource) (*model.Config, error) {
	if m == nil || m.Metadata == nil {
		return &model.Config{}, nil
	}

	c := &model.Config{
		ConfigMeta: model.ConfigMeta{
			ResourceVersion: m.Metadata.Version,
			Labels:          m.Metadata.Labels,
			Annotations:     m.Metadata.Annotations,
		},
	}
	if rev != "" && !model.ObjectInRevision(c, rev) { // In case upstream does not support rev in node meta.
		// empty rev will be considered as legacy client and NEEDS ALL CONFIGS
		return nil, nil
	}

	nsn := strings.Split(m.Metadata.Name, "/")
	if len(nsn) != 2 {
		return nil, fmt.Errorf("invalid name %s", m.Metadata.Name)
	}
	c.Namespace = nsn[0]
	c.Name = nsn[1]
	var err error
	c.CreationTimestamp, err = types.TimestampFromProto(m.Metadata.CreateTime)
	if err != nil {
		return nil, err
	}

	if m.Body != nil {
		pb, err := types.EmptyAny(m.Body)
		if err != nil {
			return nil, err
		}
		err = types.UnmarshalAny(m.Body, pb)
		if err != nil {
			return nil, err
		}
		c.Spec = pb
	}
	return c, nil
}
