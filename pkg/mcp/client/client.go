package client

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/gogo/protobuf/types"
	"github.com/golang/protobuf/ptypes/any"
	"istio.io/pkg/monitoring"

	mcp "istio.io/api/mcp/v1alpha1"
	istiolog "istio.io/pkg/log"

	"istio.io/istio-mcp/pkg/config"
	"istio.io/istio-mcp/pkg/config/schema/resource"
	"istio.io/istio-mcp/pkg/model"
)

const (
	WildcardTypeUrl    = "*"
	McpResourceTypeUrl = "type.googleapis.com/istio.mcp.v1alpha1.Resource"
)

var log = istiolog.RegisterScope("mcpc", "mcpc debugging", 0)

type Change int

func (c Change) String() string {
	switch c {
	case ChangeAdd:
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

const hookCtxValueKey = "_HOOK_CTX"

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

type DiscoveryHandler interface {
	HandleResponse(*discovery.DiscoveryResponse) error
}

type DiscoveryHandlerWrapper struct {
	ResponseHandler func(*discovery.DiscoveryResponse) error
}

func (d DiscoveryHandlerWrapper) HandleResponse(response *discovery.DiscoveryResponse) error {
	if d.ResponseHandler != nil {
		return d.ResponseHandler(response)
	}
	return nil
}

func GetConfigHandlerHookContextValue(ctx context.Context, key string) (string, bool) {
	ctxV := ctx.Value(hookCtxValueKey)
	if ctxV == nil {
		return "", false
	}

	mapV, ok := ctxV.(map[string]string)
	if !ok {
		return "", false
	}

	v, ok := mapV[key]
	return v, ok
}

func SetConfigHandlerHookContextValue(ctx context.Context, key, value string) {
	ctxV := ctx.Value(hookCtxValueKey)
	if ctxV == nil {
		return
	}

	mapV, ok := ctxV.(map[string]string)
	if !ok || mapV == nil {
		return
	}

	mapV[key] = value
}

type ConfigStoreHandlerAdapter struct {
	A *ADSC

	mut      sync.RWMutex
	initTime map[resource.GroupVersionKind]time.Time

	Inc         bool
	List        func(gvk resource.GroupVersionKind, namespace string) ([]model.NamespacedName, error)
	AddOrUpdate func(cfg model.Config) (Change, string, string, error)
	Del         func(gvk resource.GroupVersionKind, name, namespace string) error

	EnterHook func(ctx context.Context, gvk resource.GroupVersionKind, configs []*model.Config)
	ExitHook  func(ctx context.Context, gvk resource.GroupVersionKind, configs []*model.Config)
}

func (a *ConfigStoreHandlerAdapter) updateInitTime(gvk resource.GroupVersionKind) bool {
	var isInit bool
	a.mut.RLock()
	if a.initTime == nil {
		isInit = true
	} else {
		if _, ok := a.initTime[gvk]; !ok {
			isInit = true
		}
	}
	a.mut.RUnlock()
	if isInit {
		a.mut.Lock()
		if a.initTime == nil {
			a.initTime = map[resource.GroupVersionKind]time.Time{}
		}
		now := time.Now()
		a.initTime[gvk] = now
		a.mut.Unlock()
		log.Debugf("%v update init time %s", gvk, now)
	}
	return isInit
}

func (a *ConfigStoreHandlerAdapter) Reset() {
	var prev map[resource.GroupVersionKind]time.Time
	a.mut.Lock()
	prev, a.initTime = a.initTime, nil
	a.mut.Unlock()

	log.Infof("reset, prev init time: %+v", prev)
}

func (a *ConfigStoreHandlerAdapter) TypeConfigsHandler(gvk resource.GroupVersionKind, configs []*model.Config) error {
	ctx := context.WithValue(context.Background(), hookCtxValueKey, map[string]string{})
	if h := a.EnterHook; h != nil {
		h(ctx, gvk, configs)
	}
	if h := a.ExitHook; h != nil {
		defer h(ctx, gvk, configs)
	}

	var (
		isInc = a.Inc && !a.updateInitTime(gvk) // if in non-inc mode, always full; or the init/first push is full
		err   error
	)

	for _, c := range configs {
		var ch Change
		if c.Spec == nil && isInc {
			if err = a.Del(gvk, c.Name, c.Namespace); err != nil {
				log.Errorf("inc del %v %s/%s met err %v", gvk, c.Namespace, c.Name, err)
			} else {
				log.Infof("inc del %v %s/%s", gvk, c.Namespace, c.Name)
				ch = ChangeDel
			}
		} else {
			var prevRev, newRev string
			if c.Spec == nil {
				log.Errorf("inc %v add or update %v %s/%s get nil spec, skip it", isInc, gvk, c.Namespace, c.Name)
				continue
			}
			if ch, prevRev, newRev, err = a.AddOrUpdate(*c); err != nil {
				log.Errorf("inc %v add or update %v %s/%s met err %v", isInc, gvk, c.Namespace, c.Name, err)
				logConfig(c)
			} else if ch != ChangeNoUpdate {
				log.Infof("%v %s/%s change %v, prevRev %s newRev %s", gvk, c.Namespace, c.Name, ch, prevRev, newRev)
			}
		}
		if err == nil && a.A != nil {
			a.A.recordCfgChange(gvk.Kind, ch)
		}
	}

	if !isInc {
		received := make(map[model.NamespacedName]bool, len(configs))
		for _, c := range configs {
			received[model.NamespacedName{Name: c.Name, Namespace: c.Namespace}] = true
		}

		nns, curErr := a.List(gvk, "")
		if curErr != nil {
			log.Errorf("list %v met err %v", gvk, curErr)
			err = curErr
			return err
		}

		for _, nn := range nns {
			if received[nn] {
				continue
			}
			if curErr := a.Del(gvk, nn.Name, nn.Namespace); curErr != nil {
				log.Errorf("del %v %v met err %v", gvk, nn, curErr)
				err = curErr
			} else {
				log.Infof("del %v %v", gvk, nn)
				if a.A != nil {
					a.A.recordCfgChange(gvk.Kind, ChangeDel)
				}
			}
		}
	}

	return err
}

func logConfig(c *model.Config) {
	if log.DebugEnabled() {
		bs, err := json.MarshalIndent(c, "", "  ")
		log.Debuga(string(bs), err)
	}
}

type Config struct {
	Revision      string
	LocalCacheDir string

	// override default parse-resource-and-handle logic
	McpResourceHandler func([]string, *mcp.Resource) ([]*model.Config, error)
	// override default return-config-to-collect behaviour
	ConfigHandler func(*model.Config) error
	// specify how to handle configs of type
	TypeConfigsHandler func(resource.GroupVersionKind, []*model.Config) error
	InitReqTypes       []string

	// handle and convert resource to configs
	resourceHandlers map[string]map[string]func(string, string, []byte) ([]*model.Config, error)
	msgHandlers      map[string]func(*discovery.DiscoveryResponse) ([]*model.Config, error)
}

func (c *Config) RegisterResourceHandler(msgTypeUrl, resourceTypeUrl string, handler func(string, string, []byte) ([]*model.Config, error)) {
	if c.resourceHandlers == nil {
		c.resourceHandlers = map[string]map[string]func(string, string, []byte) ([]*model.Config, error){}
	}

	msgHandlers := c.resourceHandlers[msgTypeUrl]
	if msgHandlers == nil {
		msgHandlers = map[string]func(string, string, []byte) ([]*model.Config, error){}
		c.resourceHandlers[msgTypeUrl] = msgHandlers
	}

	msgHandlers[resourceTypeUrl] = handler
}

func (c *Config) RegisterMsgHandler(msgTypeUrl string, handler func(*discovery.DiscoveryResponse) ([]*model.Config, error)) {
	if c.msgHandlers == nil {
		c.msgHandlers = map[string]func(*discovery.DiscoveryResponse) ([]*model.Config, error){}
	}

	c.msgHandlers[msgTypeUrl] = handler
}

func (c *Config) MsgHandler(msgTypeUrl string) func(*discovery.DiscoveryResponse) ([]*model.Config, error) {
	if c.msgHandlers == nil {
		return nil
	}

	return c.msgHandlers[msgTypeUrl]
}

func (c *Config) ResourceHandler(msgTypeUrl, resourceTypeUrl string) func(string, string, []byte) ([]*model.Config, error) {
	if c.resourceHandlers == nil {
		return nil
	}

	msgHandlers := c.resourceHandlers[msgTypeUrl]
	if msgHandlers == nil {
		if msgTypeUrl != WildcardTypeUrl {
			msgHandlers = c.resourceHandlers[WildcardTypeUrl]
		}
		if msgHandlers == nil {
			return nil
		}
	}

	h := msgHandlers[resourceTypeUrl]
	if h != nil || resourceTypeUrl == WildcardTypeUrl {
		return h
	}
	return msgHandlers[WildcardTypeUrl]
}

func typeName(typeUrl string) string {
	if idx := strings.LastIndex(typeUrl, "/"); idx >= 0 {
		typeUrl = typeUrl[idx+1:]
	}

	if idx := strings.LastIndex(typeUrl, "."); idx >= 0 {
		typeUrl = typeUrl[idx+1:]
	}
	return typeUrl
}

type ADSC struct {
	config           *Config
	mut              sync.Mutex
	metricCfgChanges [ChangeLen]map[string]monitoring.Metric
}

func NewAdsc(config *Config) *ADSC {
	ret := &ADSC{
		config: config,
	}
	for _, r := range config.InitReqTypes {
		tn := typeName(r)

		for i := 0; i < int(ChangeLen); i++ {
			m := ret.metricCfgChanges[i]
			if m == nil {
				m = map[string]monitoring.Metric{}
				ret.metricCfgChanges[i] = m
			}
			if _, ok := m[tn]; !ok {
				m[tn] = metricAdscChanges[i].With(cfgTag.Value(tn))
			}
		}
	}

	ret.config.RegisterResourceHandler(WildcardTypeUrl, McpResourceTypeUrl, func(msgTypeUrl string, resourceTypeUrl string, bytes []byte) ([]*model.Config, error) {
		configs, err := ret.handleMCP(resource.TypeUrlToGvkTuple(msgTypeUrl), &any.Any{
			TypeUrl: resourceTypeUrl,
			Value:   bytes,
		}, bytes)
		if err != nil {
			log.Warnf("Error handling received MCP config %v", err)
			return nil, err
		}
		return configs, nil
	})
	return ret
}

func (a *ADSC) HandleResponse(disResp *discovery.DiscoveryResponse) error {
	msg := disResp

	log.Infof("Received type %s cnt-%d nonce=%s versionInfo=%s", msg.TypeUrl, len(msg.Resources), msg.Nonce, msg.VersionInfo)
	var (
		err         error
		typeConfigs []*model.Config
	)

	handler := a.config.MsgHandler(msg.TypeUrl)
	if handler != nil {
		if typeConfigs, err = handler(disResp); err != nil {
			return err
		}
	} else {
		// Process the resources.
		for _, rsc := range msg.Resources { // Any
			// a.VersionInfo[rsc.TypeUrl] = msg.VersionInfo
			resourceHandler := a.config.ResourceHandler(msg.TypeUrl, rsc.TypeUrl)
			if resourceHandler == nil {
				continue
			}

			configs, err := resourceHandler(msg.TypeUrl, rsc.TypeUrl, rsc.Value)
			if err != nil {
				return err
			}
			typeConfigs = append(typeConfigs, configs...)
		}
	}

	if a.config.TypeConfigsHandler != nil {
		return a.config.TypeConfigsHandler(resource.TypeUrlToGvk(msg.TypeUrl), typeConfigs)
	}

	return nil
	// XXX handle envoy resources?
}

func (a *ADSC) handleMCP(gvk []string, rsc *any.Any, valBytes []byte) ([]*model.Config, error) {
	if len(gvk) != 3 {
		return nil, nil // Not MCP
	}

	m := &mcp.Resource{}
	err := types.UnmarshalAny(&types.Any{
		TypeUrl: rsc.TypeUrl,
		Value:   rsc.Value,
	}, m)
	if err != nil {
		return nil, err
	}

	if a.config.McpResourceHandler != nil {
		return a.config.McpResourceHandler(gvk, m)
	}

	newCfg, err := config.McpToPilot(a.config.Revision, m)
	if err != nil {
		log.Warnf("Invalid data %s of %v, err %v", string(valBytes), gvk, err)
		return nil, err
	}
	if newCfg == nil {
		return nil, nil
	}

	newCfg.GroupVersionKind = resource.GroupVersionKind{Group: gvk[0], Version: gvk[1], Kind: gvk[2]}
	if a.config.LocalCacheDir != "" {
		defer func() error {
			strResponse, err := json.MarshalIndent(newCfg, "  ", "  ")
			if err != nil {
				return err
			}
			err = ioutil.WriteFile(a.config.LocalCacheDir+"_res."+
				newCfg.GroupVersionKind.Kind+"."+newCfg.Namespace+"."+newCfg.Name+".json", strResponse, 0o644)

			return err
		}()
	}

	if a.config.ConfigHandler != nil {
		if err := a.config.ConfigHandler(newCfg); err != nil {
			return nil, err
		}
	}
	return []*model.Config{newCfg}, nil
}

func (a *ADSC) recordCfgChange(typeUrl string, ch Change) {
	if ch < 0 || ch >= ChangeLen {
		return
	}

	m := a.metricCfgChanges[ch]
	if m == nil {
		return
	}

	tn := typeName(typeUrl)
	if metric := m[tn]; metric != nil {
		metric.Increment()
	}
}
