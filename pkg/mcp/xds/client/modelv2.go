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
	apiTypePrefixV2       = apiTypePrefix + "envoy.api.v2."
	discoveryTypePrefixV2 = "type.googleapis.com/envoy.service.discovery.v2."
	// ClusterType is used for cluster discovery. Typically first request received
	V2ClusterType = apiTypePrefixV2 + ClusterType
	// EndpointType is used for EDS and ADS endpoint discovery. Typically second request.
	V2EndpointType = apiTypePrefixV2 + EndpointType
	// ListenerType is sent after clusters and endpoints.
	V2ListenerType = apiTypePrefixV2 + ListenerType
	// RouteType is sent after listeners.
	V2RouteType = apiTypePrefixV2 + RouteType
)
