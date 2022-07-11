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

package xds

import (
	"istio.io/istio-mcp/pkg/features"
	"strings"

	gogotypes "github.com/gogo/protobuf/types"
	golangany "github.com/golang/protobuf/ptypes/any"

	mcp "istio.io/api/mcp/v1alpha1"
	"istio.io/pkg/log"

	"istio.io/istio-mcp/pkg/config/schema/resource"
	"istio.io/istio-mcp/pkg/model"
)

// Experimental/WIP: this is not yet ready for production use.
// You can continue to use 1.5 Galley until this is ready.
//
// APIGenerator supports generation of high-level API resources, similar with the MCP
// protocol. This is a replacement for MCP, using XDS (and in future UDPA) as a transport.
// Based on lessons from MCP, the protocol allows incremental updates by
// default, using the same mechanism that EDS is using, i.e. sending only changed resources
// in a push. Incremental deletes are sent as a resource with empty body.
//
// Example: networking.istio.io/v1alpha3/VirtualService
//
// TODO: we can also add a special marker in the header)
type APIGenerator struct {
	incPush bool
}

// TODO: take 'updates' into account, don't send pushes for resources that haven't changed
// TODO: support WorkloadEntry - to generate endpoints (equivalent with EDS)
// TODO: based on lessons from MCP, we want to send 'chunked' responses, like apiserver does.
// A first attempt added a 'sync' record at the end. Based on feedback and common use, a
// different approach can be used - for large responses, we can mark the last one as 'hasMore'
// by adding a field to the envelope.

// Generate implements the generate method for high level APIs, like Istio config types.
// This provides similar functionality with MCP and :8080/debug/configz.
//
// Names are based on the current resource naming in istiod stores.
func (g *APIGenerator) Generate(proxy *Proxy, push *PushContext, w *WatchedResource, updates XdsUpdates) Resources {
	res := []*golangany.Any{}
	var ver string
	if g.incPush {
		ver = w.NonceSent
	}
	newVer := ver

	// Note: this is the style used by MCP and its config. Pilot is using 'Group/Version/Kind' as the
	// key, which is similar.
	//
	// The actual type in the Any should be a real proto - which is based on the generated package name.
	// For example: type is for Any is 'type.googlepis.com/istio.networking.v1alpha3.EnvoyFilter
	// We use: networking.istio.io/v1alpha3/EnvoyFilter
	kind := strings.SplitN(w.TypeUrl, "/", 3)
	if len(kind) != 3 {
		log.Warnf("ADS: Unknown watched resources %s", w.TypeUrl)
		// Still return an empty response - to not break waiting code. It is fine to not know about some resource.
		return Resources{Version: newVer}
	}
	// TODO: extra validation may be needed - at least logging that a resource
	// of unknown type was requested. This should not be an error - maybe client asks
	// for a valid CRD we just don't know about. An empty set indicates we have no such config.
	rgvk := resource.GroupVersionKind{
		Group:   kind[0],
		Version: kind[1],
		Kind:    kind[2],
	}
	// XXX Leave to impl.
	//if w.TypeUrl == collections.IstioMeshV1Alpha1MeshConfig.Resource().GroupVersionKind().String() {
	//	meshAny, err := gogotypes.MarshalAny(push.Mesh)
	//	if err == nil {
	//		res = append(res, &golangany.Any{
	//			TypeUrl: meshAny.TypeUrl,
	//			Value:   meshAny.Value,
	//		})
	//	}
	//	return res
	//}

	// TODO: what is the proper way to handle errors ?
	// Normally once istio is 'ready' List can't return errors on a valid config -
	// even if k8s is disconnected, we still cache all previous results.
	// This needs further consideration - I don't think XDS or MCP transports
	// have a clear recommendation.

	cfg, err := push.ConfigStore.List(rgvk, "", ver)
	if err != nil {
		log.Warnf("ADS: Error reading resource %s %v", w.TypeUrl, err)
		return Resources{Version: newVer}
	}

	for _, c := range cfg {
		if g.incPush && c.Spec == nil && w.NonceSent == "" {
			// init push do not need nil-spec items which are used to indicate deletion.
			continue
		}

		if proxy.Metadata.IstioRevision != "" {
			// or we consider it's a legacy client and leave the filter-by-rev thing to itself
			if revMatch := model.ObjectInRevision(&c, proxy.Metadata.IstioRevision); !revMatch {
				// NOTE: incPush can not handle the rev-change case.
				if features.ExtraRevMap != nil { // for downwards compatibility
					mappedRev, ok := features.ExtraRevMap[c.Labels[model.IstioRevLabel]]
					if ok && model.RevisionMatch(mappedRev, proxy.Metadata.IstioRevision) {
						revMatch = true
						labelsCopy := make(map[string]string, len(c.Labels))
						for k, v := range c.Labels {
							labelsCopy[k] = v
						}
						labelsCopy[model.IstioRevLabel] = mappedRev
						c.Labels = labelsCopy
					}
				}
				if !revMatch {
					continue
				}
			}
		}

		// Right now model.Config is not a proto - until we change it, mcp.Resource.
		// This also helps migrating MCP users.
		if cRev := c.CurrentResourceVersion(); cRev > newVer {
			newVer = cRev
		}

		b, err := configToResource(&c)
		if err != nil {
			log.Warna("Resource error ", err, " ", c.Namespace, "/", c.Name)
			continue
		}
		bany, err := gogotypes.MarshalAny(b)
		if err == nil {
			res = append(res, &golangany.Any{
				TypeUrl: bany.TypeUrl,
				Value:   bany.Value,
			})
		} else {
			log.Warna("Any ", err)
		}
	}

	// TODO: MeshConfig, current dynamic ProxyConfig (for this proxy), Networks

	return Resources{
		Data:    res,
		Version: newVer,
	}
}

// Convert from model.Config, which has no associated proto, to MCP Resource proto.
// TODO: define a proto matching Config - to avoid useless superficial conversions.
func configToResource(c *model.Config) (*mcp.Resource, error) {
	r := &mcp.Resource{}

	if c.Spec != nil {
		// MCP, K8S and Istio configs use gogo configs
		// On the wire it's the same as golang proto.
		a, err := gogotypes.MarshalAny(c.Spec)
		if err != nil {
			return nil, err
		}
		r.Body = a
	}
	ts, err := gogotypes.TimestampProto(c.CreationTimestamp)
	if err != nil {
		return nil, err
	}
	r.Metadata = &mcp.Metadata{
		Name:        c.Namespace + "/" + c.Name,
		CreateTime:  ts,
		Version:     c.ResourceVersion,
		Labels:      c.Labels,
		Annotations: c.Annotations,
	}

	return r, nil
}
