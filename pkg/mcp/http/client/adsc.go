package client

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/proto"

	"istio.io/istio-mcp/pkg/config/schema/resource"
	mcpclient "istio.io/istio-mcp/pkg/mcp/client"
	mcphttp "istio.io/istio-mcp/pkg/mcp/http"
	mcpmodel "istio.io/istio-mcp/pkg/model"
	"istio.io/libistio/pkg/log"
)

const (
	DefaultFaultGap    = 5 * time.Second
	DefaultPollTimeout = mcphttp.DefaultLongPollingInterval + 5*time.Second
)

type Config struct {
	FaultGap    time.Duration
	PollTimeout time.Duration
	URL         string
	InitVersion string
	Namespace   string // "" means all.

	Resources []resource.GroupVersionKind

	DiscoveryHandler mcpclient.DiscoveryHandler
}

func ParseConfig(urlStr string) (*Config, error) {
	srcAddress, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("invalid http config URL %s %v", urlStr, err)
	}
	var (
		faultGap    = DefaultFaultGap
		pollTimeout = DefaultPollTimeout
	)
	if v := srcAddress.Query().Get("faultGap"); v != "" {
		if d, err := time.ParseDuration(v); err != nil {
			return nil, err
		} else {
			faultGap = d
		}
	}
	if v := srcAddress.Query().Get("pollTimeout"); v != "" {
		if d, err := time.ParseDuration(v); err != nil {
			return nil, err
		} else {
			pollTimeout = d
		}
	}

	return &Config{
		FaultGap:    faultGap,
		PollTimeout: pollTimeout,
		URL:         urlStr,
	}, nil
}

type ADSC struct {
	config *Config

	versionInfo  map[string]string
	mut          sync.Mutex
	faultBackoff backoff.BackOff
}

func NewAdsc(config *Config) *ADSC {
	ret := &ADSC{
		config:       config,
		versionInfo:  map[string]string{},
		faultBackoff: backoff.NewConstantBackOff(config.FaultGap),
	}

	return ret
}

func (a *ADSC) RunWithCtx(ctx context.Context) {
	for _, gvk := range a.config.Resources {
		go a.pollTask(ctx, gvk)
	}
}

func (a *ADSC) pollTask(ctx context.Context, gvk resource.GroupVersionKind) {
	ver := a.config.InitVersion
	typeUrl := gvk.String()

	for {
		curCtx, cancel := context.WithTimeout(ctx, a.config.PollTimeout)
		disResp, curVer, err := a.poll(ver, gvk, curCtx)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Client timeout. Do nothing
				log.Infof("httpadsc poll task for %s timeout, cur ver %s", typeUrl, ver)
			} else {
				log.Errorf("httpadsc poll task for %s met error %v, prev version %s, cur version %s", typeUrl, err, ver, curVer)
				time.Sleep(a.faultBackoff.NextBackOff())
			}
		} else if curVer == mcpmodel.VersionNotInitialized || curVer == ver {
			// server no change but timeout, do nothing
			if curVer == mcpmodel.VersionNotInitialized {
				log.Infof("httpadsc poll task for %s return not init ver %s while cur ver is %s", typeUrl, ver, curVer)
			} else {
				log.Infof("httpadsc poll task for %s return with same ver ver %s %s", typeUrl, ver, curVer)
			}
		} else if disResp == nil {
			log.Warnf("httpadsc poll task for %s return nil resp, ver %s cur ver is %s", typeUrl, ver, curVer)
			time.Sleep(a.faultBackoff.NextBackOff())
		} else {
			log.Infof("httpadsc %s prev version %s, new version: %s", typeUrl, ver, curVer)
			if a.config.DiscoveryHandler == nil {
				cancel()
				continue
			}
			if err := a.config.DiscoveryHandler.HandleResponse(disResp); err != nil {
				log.Errorf("httpadsc %s met error %v, prev version %s, cur version %s", typeUrl, err, ver, curVer)
				time.Sleep(a.faultBackoff.NextBackOff())
			} else {
				ver = curVer
				a.mut.Lock()
				a.versionInfo[typeUrl] = ver
				a.mut.Unlock()
			}
		}
		// cancel the curContext
		cancel()
	}
}

func (a *ADSC) poll(ver string, gvk resource.GroupVersionKind, ctx context.Context) (*discovery.DiscoveryResponse, string, error) {
	adsUrl, err := url.Parse(a.config.URL)
	if err != nil {
		return nil, "", fmt.Errorf("invalid url %v", a.config.URL)
	}
	q := adsUrl.Query()
	q.Add("version", ver)
	q.Add("typeUrl", gvk.String())
	q.Add("ns", a.config.Namespace)
	adsUrl.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, adsUrl.String(), nil)
	if err != nil {
		return nil, "", err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, "", err
	}

	version := resp.Header.Get("version")
	bs, err := io.ReadAll(resp.Body) // should read all
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("http status %d not ok", resp.StatusCode)
	}
	if version == mcpmodel.VersionNotInitialized {
		return nil, version, nil
	}

	var disResp discovery.DiscoveryResponse
	if version == ver {
		return nil, version, nil
	}

	if err := proto.Unmarshal(bs, &disResp); err != nil {
		return nil, version, err
	}

	return &disResp, version, nil
}

// impl k8s controller
func (a *ADSC) Run(stopCh <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	a.RunWithCtx(ctx)
	go func() {
		<-stopCh
		cancel()
	}()
}

func (a *ADSC) HasSynced() bool {
	a.mut.Lock()
	defer a.mut.Unlock()

	return len(a.versionInfo) == len(a.config.Resources)
}

func (a *ADSC) LastSyncResourceVersion() string {
	a.mut.Lock()
	defer a.mut.Unlock()

	var ret string
	for _, ver := range a.versionInfo {
		if strings.Compare(ver, ret) > 0 {
			ret = ver
		}
	}
	return ret
}
