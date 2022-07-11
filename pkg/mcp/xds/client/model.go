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

const (
	apiTypePrefix = "type.googleapis.com/"

	EndpointType = "ClusterLoadAssignment"
	ClusterType  = "Cluster"
	RouteType    = "RouteConfiguration"
	ListenerType = "Listener"

	V3ClusterType  = apiTypePrefix + "envoy.config.cluster.v3." + ClusterType
	V3EndpointType = apiTypePrefix + "envoy.config.endpoint.v3." + EndpointType
	V3ListenerType = apiTypePrefix + "envoy.config.listener.v3." + ListenerType
	V3RouteType    = apiTypePrefix + "envoy.config.route.v3." + RouteType
)

var (
	ListenerShortType = "LDS"
	RouteShortType    = "RDS"
	EndpointShortType = "EDS"
	ClusterShortType  = "CDS"
)

func GetShortType(typeURL string) string {
	switch typeURL {
	case V2ClusterType, V3ClusterType:
		return ClusterShortType
	case V2ListenerType, V3ListenerType:
		return ListenerShortType
	case V2RouteType, V3RouteType:
		return RouteShortType
	case V2EndpointType, V3EndpointType:
		return EndpointShortType
	default:
		return typeURL
	}
}
