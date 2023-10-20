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

package model

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"istio.io/istio-mcp/pkg/features"

	udpa "github.com/cncf/udpa/go/udpa/type/v1"
	"istio.io/istio-mcp/pkg/config/schema/resource"
)

const (
	VersionNotInitialized     = ""
	AnnotationResourceVersion = "ResourceVersion"

	IstioRevLabel = "istio.io/rev" // does not refer to that in istio-api to get a better compatibility
)

// Statically link protobuf descriptors from UDPA
var _ = udpa.TypedStruct{}

type NamespacedName struct {
	Name      string
	Namespace string
}

// ConfigKey describe a specific config item.
// In most cases, the name is the config's name. However, for ServiceEntry it is service's FQDN.
type ConfigKey struct {
	Kind      resource.GroupVersionKind
	Name      string
	Namespace string
}

func (key ConfigKey) HashCode() uint32 {
	var result uint32
	result = 31*result + crc32.ChecksumIEEE([]byte(key.Kind.Kind))
	result = 31*result + crc32.ChecksumIEEE([]byte(key.Kind.Version))
	result = 31*result + crc32.ChecksumIEEE([]byte(key.Kind.Group))
	result = 31*result + crc32.ChecksumIEEE([]byte(key.Namespace))
	result = 31*result + crc32.ChecksumIEEE([]byte(key.Name))
	return result
}

// ConfigsOfKind extracts configs of the specified kind.
func ConfigsOfKind(configs map[ConfigKey]struct{}, kind resource.GroupVersionKind) map[ConfigKey]struct{} {
	ret := make(map[ConfigKey]struct{})

	for conf := range configs {
		if conf.Kind == kind {
			ret[conf] = struct{}{}
		}
	}

	return ret
}

// ConfigNamesOfKind extracts config names of the specified kind.
func ConfigNamesOfKind(configs map[ConfigKey]struct{}, kind resource.GroupVersionKind) map[string]struct{} {
	ret := make(map[string]struct{})

	for conf := range configs {
		if conf.Kind == kind {
			ret[conf.Name] = struct{}{}
		}
	}

	return ret
}

// ConfigMeta is metadata attached to each configuration unit.
// The revision is optional, and if provided, identifies the
// last update operation on the object.
type ConfigMeta struct {
	// GroupVersionKind is a short configuration name that matches the content message type
	// (e.g. "route-rule")
	GroupVersionKind resource.GroupVersionKind `json:"type,omitempty"`

	// Name is a unique immutable identifier in a namespace
	Name string `json:"name,omitempty"`

	// Namespace defines the space for names (optional for some types),
	// applications may choose to use namespaces for a variety of purposes
	// (security domains, fault domains, organizational domains)
	Namespace string `json:"namespace,omitempty"`

	// Domain defines the suffix of the fully qualified name past the namespace.
	// Domain is not a part of the unique key unlike name and namespace.
	Domain string `json:"domain,omitempty"`

	// Map of string keys and values that can be used to organize and categorize
	// (scope and select) objects.
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations is an unstructured key value map stored with a resource that may be
	// set by external tools to store and retrieve arbitrary metadata. They are not
	// queryable and should be preserved when modifying objects.
	Annotations map[string]string `json:"annotations,omitempty"`

	// ResourceVersion is an opaque identifier for tracking updates to the config registry.
	// The implementation may use a change index or a commit log for the revision.
	// The config client should not make any assumptions about revisions and rely only on
	// exact equality to implement optimistic concurrency of read-write operations.
	//
	// The lifetime of an object of a particular revision depends on the underlying data store.
	// The data store may compactify old revisions in the interest of storage optimization.
	//
	// An empty revision carries a special meaning that the associated object has
	// not been stored and assigned a revision.
	ResourceVersion string `json:"resourceVersion,omitempty"`

	// CreationTimestamp records the creation time
	CreationTimestamp time.Time `json:"creationTimestamp,omitempty"`
}

func SerializeConfigMeta(cm ConfigMeta) []byte {
	buf := &bytes.Buffer{}

	sep := func() {
		buf.WriteByte(0)
	}

	addStr := func(strs ...string) {
		for _, s := range strs {
			buf.WriteString(s)
			sep()
		}
	}
	addStrMap := func(m map[string]string) {
		if len(m) > 0 {
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, k := range keys {
				addStr(k, m[k])
			}
		}
		sep()
	}

	addStr(cm.GroupVersionKind.Group, cm.GroupVersionKind.Version, cm.GroupVersionKind.Kind,
		cm.Name, cm.Namespace, cm.Domain, cm.ResourceVersion)

	addStrMap(cm.Labels)
	addStrMap(cm.Annotations)

	intBuf := [8]byte{}
	binary.BigEndian.PutUint64(intBuf[:], uint64(cm.CreationTimestamp.UnixNano()))
	buf.Write(intBuf[:])

	return buf.Bytes()
}

// Config is a configuration unit consisting of the type of configuration, the
// key identifier that is unique per type, and the content represented as a
// protobuf message.
type Config struct {
	ConfigMeta

	// Spec holds the configuration object as a gogo protobuf message
	Spec proto.Message
}

func (c *Config) CurrentResourceVersion() string {
	// c.ResourceVersion: prev
	// resource version in annotation: curr/new
	if annos := c.Annotations; annos == nil {
		return ""
	} else {
		return annos[AnnotationResourceVersion]
	}
}

// UpdateLabel see updateLabelOrAnno
func (c *Config) UpdateLabel(k, v string) bool {
	return c.updateLabelOrAnno(&c.Labels, k, v)
}

// UpdateAnnotation see updateLabelOrAnno
func (c *Config) UpdateAnnotation(k, v string) bool {
	return c.updateLabelOrAnno(&c.Annotations, k, v)
}

// updateLabelOrAnno do a cow update when necessary and return whether actually updated
func (c *Config) updateLabelOrAnno(mp *map[string]string, k, v string) bool {
	m := *mp
	if m[k] == v { // consider empty value equals not-exist
		return false
	}

	if m == nil {
		m = map[string]string{}
	} else {
		mCopy := make(map[string]string, len(m))
		for mk, mv := range m {
			mCopy[mk] = mv
		}
		m = mCopy
	}

	if v == "" {
		delete(m, k)
	} else {
		m[k] = v
	}
	*mp = m
	return true
}

func (c *Config) UpdateAnnotationResourceVersion() string {
	// check if c.ResourceVersion has changed (different from ver in anno)
	// update version in annotation if that
	annos := c.Annotations
	var prev string
	if annos == nil {
		if c.ResourceVersion == prev {
			return prev
		}
		// new
		annos = map[string]string{}
	} else {
		prev = annos[AnnotationResourceVersion]
		if c.ResourceVersion == prev {
			return prev
		}
		// cow to avoid concurrent-access
		annos = make(map[string]string, len(c.Annotations))
		for k, v := range c.Annotations {
			annos[k] = v
		}
	}

	annos[AnnotationResourceVersion] = c.ResourceVersion
	c.Annotations = annos
	return prev
}

// Key function for the configuration objects
func Key(typ, name, namespace string) string {
	return fmt.Sprintf("%s/%s/%s", typ, namespace, name)
}

// Key is the unique identifier for a configuration object
// TODO: this is *not* unique - needs the version and group
func (meta *ConfigMeta) Key() string {
	return Key(meta.GroupVersionKind.Kind, meta.Name, meta.Namespace)
}

func (meta *ConfigMeta) ConfigKey() ConfigKey {
	return ConfigKey{meta.GroupVersionKind, meta.Name, meta.Namespace}
}

func (c Config) DeepCopy() Config {
	var clone Config
	clone.ConfigMeta = c.ConfigMeta
	clone.Spec = proto.Clone(c.Spec)
	return clone
}

type ConfigStore interface {
	Get(gvk resource.GroupVersionKind, namespace, name string) (*Config, error)
	List(gvk resource.GroupVersionKind, namespace, ver string) ([]Config, string, error)
	// Snapshot if pass all-namespace, will return a snapshot contains all ns's data and use the largest
	// version of them as version.
	Snapshot(ns string) ConfigSnapshot
	// Version if pass all-namespace, will return the largest version of all namespace snapshots.
	Version(ns string) string
	// VersionSnapshot
	// version can not be empty or return nil.
	// Ns can be empty to indicate merging-all-contents-of-same-version-to-one-snapshot.
	VersionSnapshot(version, ns string) ConfigSnapshot
}

// ConfigSnapshot may merge configs of different namespaces but same version into a single snapshot.
type ConfigSnapshot interface {
	Version() string
	Config(gvk resource.GroupVersionKind, namespace, name string) *Config
	Configs(gvk resource.GroupVersionKind, namespace, ver string) []Config
	Empty() bool
}

type NsConfigSnapshot struct {
	snapshots map[string]ConfigSnapshot
}

func MakeNsConfigSnapshot(snapshots map[string]ConfigSnapshot) NsConfigSnapshot {
	return NsConfigSnapshot{snapshots: snapshots}
}

func (n NsConfigSnapshot) Snapshots() map[string]ConfigSnapshot {
	ret := make(map[string]ConfigSnapshot, len(n.snapshots))
	for k, v := range n.snapshots {
		ret[k] = v
	}
	return ret
}

func (n NsConfigSnapshot) Version() string {
	var ret string
	for _, snapshot := range n.snapshots {
		if snapshot == nil {
			continue
		}
		ver := snapshot.Version()
		if strings.Compare(ver, ret) > 0 {
			ret = ver
		}
	}

	return ret
}

func (n NsConfigSnapshot) Config(gvk resource.GroupVersionKind, namespace, name string) *Config {
	snap := n.snapshots[namespace]
	if snap == nil {
		return nil
	}
	return snap.Config(gvk, namespace, name)
}

func (n NsConfigSnapshot) Configs(gvk resource.GroupVersionKind, namespace, ver string) []Config {
	if namespace != "" {
		snapshot := n.snapshots[namespace]
		if snapshot == nil {
			return nil
		}
		return snapshot.Configs(gvk, namespace, ver)
	}

	var ret []Config
	for ns, snapshot := range n.snapshots {
		if snapshot == nil {
			continue
		}

		ret = append(ret, snapshot.Configs(gvk, ns, ver)...)
	}

	return ret
}

func (n NsConfigSnapshot) Empty() bool {
	for _, snapshot := range n.snapshots {
		if snapshot == nil {
			continue
		}
		if !snapshot.Empty() {
			return false
		}
	}
	return true
}

type SimpleConfigSnapshot struct {
	version string
	configs []Config
}

func MakeSimpleConfigSnapshot(version string, configs []Config) SimpleConfigSnapshot {
	return SimpleConfigSnapshot{version, configs}
}

func (s SimpleConfigSnapshot) Empty() bool {
	return len(s.configs) == 0
}

func (s SimpleConfigSnapshot) Version() string {
	return s.version
}

// Config not that efficient
func (s SimpleConfigSnapshot) Config(gvk resource.GroupVersionKind, namespace, name string) *Config {
	for _, cfg := range s.configs {
		if cfg.Name == name && cfg.Namespace == namespace && cfg.GroupVersionKind == gvk {
			return &cfg
		}
	}
	return nil
}

func (s SimpleConfigSnapshot) Configs(gvk resource.GroupVersionKind, namespace, ver string) []Config {
	return FilterByGvkAndNamespace(s.configs, gvk, namespace, ver)
}

type SimpleConfigStore struct {
	sync.RWMutex
	snaps map[string]ConfigSnapshot
}

func NewSimpleConfigStore() *SimpleConfigStore {
	return &SimpleConfigStore{
		snaps: map[string]ConfigSnapshot{},
	}
}

func (s *SimpleConfigStore) Update(ns string, snap ConfigSnapshot) ConfigSnapshot {
	s.Lock()
	defer s.Unlock()

	prev := s.snaps[ns]
	if snap == nil {
		if prev != nil {
			delete(s.snaps, ns)
		}
	} else {
		s.snaps[ns] = snap
	}
	return prev
}

func (s *SimpleConfigStore) Version(ns string) string {
	s.RLock()
	defer s.RUnlock()

	ret := VersionNotInitialized

	if ns == resource.AllNamespace {
		for _, snap := range s.snaps {
			ver := snap.Version()
			if strings.Compare(ver, ret) > 0 {
				ret = ver
			}
		}
	} else {
		snap := s.snaps[ns]
		if snap != nil {
			ret = snap.Version()
		}
	}

	return ret
}

func (s *SimpleConfigStore) Get(gvk resource.GroupVersionKind, namespace, name string) (*Config, error) {
	s.RLock()
	snap := s.snaps[namespace]
	s.RUnlock()

	if snap == nil {
		return nil, nil
	}

	return snap.Config(gvk, namespace, name), nil
}

func (s *SimpleConfigStore) List(gvk resource.GroupVersionKind, namespace, ver string) ([]Config, string, error) {
	var (
		ret    []Config
		retVer string
	)

	addConfigs := func(cfgs ...Config) {
		for _, cfg := range cfgs {
			if cfg.ResourceVersion > retVer {
				retVer = cfg.ResourceVersion
			}
			ret = append(ret, cfg)
		}
	}

	s.RLock()
	defer s.RUnlock()

	if namespace == resource.AllNamespace {
		for _, snap := range s.snaps {
			addConfigs(snap.Configs(gvk, namespace, ver)...)
		}
	} else {
		snap := s.snaps[namespace]
		if snap != nil {
			addConfigs(snap.Configs(gvk, namespace, ver)...)
		}
	}

	return ret, retVer, nil
}

func (s *SimpleConfigStore) Snapshot(ns string) ConfigSnapshot {
	s.RLock()
	defer s.RUnlock()

	if ns == resource.AllNamespace {
		// copy
		cp := make(map[string]ConfigSnapshot, len(s.snaps))
		for k, v := range s.snaps {
			cp[k] = v
		}
		return MakeNsConfigSnapshot(cp)
	}

	return s.snaps[ns]
}

func (s *SimpleConfigStore) VersionSnapshot(version, ns string) ConfigSnapshot {
	snap := s.Snapshot(ns)
	if snap == nil || snap.Version() != version {
		return nil
	}
	return snap
}

type RecordEmptyConfigStore struct {
	*SimpleConfigStore
	lastN         int
	emptyVersions map[string][]string
	sync.RWMutex
}

func NewRecordEmptyConfigStore(store *SimpleConfigStore, lastN int) *RecordEmptyConfigStore {
	if store == nil {
		store = NewSimpleConfigStore()
	}
	return &RecordEmptyConfigStore{SimpleConfigStore: store, lastN: lastN, emptyVersions: map[string][]string{}}
}

func (s *RecordEmptyConfigStore) Version(ns string) string {
	return s.SimpleConfigStore.Version(ns)
}

func (s *RecordEmptyConfigStore) VersionSnapshot(version, ns string) ConfigSnapshot {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()

	snap := s.SimpleConfigStore.VersionSnapshot(version, ns)
	if snap != nil {
		return snap
	}

	// check for known empty snaps
	for _, r := range s.emptyVersions[ns] {
		if r == version {
			return SimpleConfigSnapshot{
				version: version,
				configs: []Config{},
			}
		}
	}

	return nil
}

func (s *RecordEmptyConfigStore) Update(ns string, snapshot ConfigSnapshot) ConfigSnapshot {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()

	if ns == resource.AllNamespace {
		return nil
	}

	ret := s.SimpleConfigStore.Update(ns, snapshot)

	if s.lastN > 0 {
		var affectedNs [][2]string
		if snapshot == nil || snapshot.Empty() { // only delete or update to empty will cause all-ns snap to be empty.
			allNsSnap := s.SimpleConfigStore.Snapshot(resource.AllNamespace)
			if allNsSnap != nil && allNsSnap.Empty() {
				affectedNs = append(affectedNs, [2]string{resource.AllNamespace, allNsSnap.Version()})
			}
		}
		if snapshot != nil && snapshot.Empty() {
			affectedNs = append(affectedNs, [2]string{ns, snapshot.Version()})
		}

		for _, item := range affectedNs {
			ns, ver := item[0], item[1]
			record := s.emptyVersions[ns]
			if record == nil {
				record = make([]string, s.lastN)
				record[0] = ver
				s.emptyVersions[ns] = record
			} else {
				last := ver
				for i := 0; i < len(record); i++ {
					record[i], last = last, record[i]
				}
			}
		}
	}

	return ret
}

func FilterByGvkAndNamespace(configs []Config, gvk resource.GroupVersionKind, namespace, ver string) []Config {
	var (
		allGvk = gvk == resource.AllGvk
		allNs  = namespace == resource.AllNamespace
	)

	if allGvk && allNs {
		return configs
	}

	matcher := func(c Config) bool {
		return (allGvk || c.GroupVersionKind == gvk) && (allNs || c.Namespace == namespace) && (ver == "" || c.ResourceVersion > ver)
	}

	var ret []Config
	for _, c := range configs {
		if matcher(c) {
			ret = append(ret, c)
		}
	}
	return ret
}

func ObjectInRevision(c *Config, rev string) bool {
	return RevisionMatch(ConfigIstioRev(c), rev)
}

func ConfigIstioRev(c *Config) string {
	return c.Labels[IstioRevLabel]
}

func RevisionMatch(configRev, rev string) bool {
	if configRev == "" { // This is a global object
		return !features.StrictIstioRev || // global obj always included in non-strict mode
			configRev == rev
	}

	// Otherwise, only return true if ","-joined rev contains config rev
	for rev != "" {
		idx := strings.Index(rev, ",")
		var cur string
		if idx >= 0 {
			cur, rev = rev[:idx], rev[idx+1:]
		} else {
			cur, rev = rev, ""
		}

		if configRev == cur {
			return true
		}
	}
	return false
}

// UpdateConfigToProxyRevision try to update config rev to the first non-empty proxy rev
func UpdateConfigToProxyRevision(c *Config, proxyRev string) bool {
	// find first non-empty rev
	rev := proxyRev
	for idx := strings.Index(proxyRev, ","); idx >= 0; {
		rev = proxyRev[:idx]
		proxyRev = proxyRev[idx+1:]
		if rev != "" {
			break
		}
	}

	return c.UpdateLabel(IstioRevLabel, rev)
}
