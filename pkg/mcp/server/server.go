package server

import (
	"fmt"
	"strings"

	"istio.io/istio-mcp/pkg/mcp"
	mcphttp "istio.io/istio-mcp/pkg/mcp/http"
	mcpxds "istio.io/istio-mcp/pkg/mcp/xds"
	"istio.io/libistio/pkg/log"
)

type Options struct {
	HttpServerOptions      *mcphttp.ServerOptions
	HttpServerExtraOptions *mcphttp.ServerExtraOptions
	XdsServerOptions       *mcpxds.ServerOptions

	ServerUrl string
}

func NewServer(o *Options) (mcp.Server, error) {
	cp := *o

	if cp.ServerUrl != "" {
		switch {
		case strings.HasPrefix(o.ServerUrl, "http://"):
			if cp.HttpServerOptions == nil {
				cp.HttpServerOptions = mcphttp.DefaultServerOptions()
			}
			err := mcphttp.ParseIntoServerOptions(cp.ServerUrl, cp.HttpServerOptions)
			if err != nil {
				return nil, err
			}
		case strings.HasPrefix(o.ServerUrl, "xds://"):
			if cp.XdsServerOptions == nil {
				cp.XdsServerOptions = mcpxds.DefaultServerOptions()
			}
			err := mcpxds.ParseIntoServerOptions(cp.ServerUrl, cp.XdsServerOptions)
			if err != nil {
				return nil, err
			}
		}
	}

	if cp.HttpServerOptions == nil && cp.XdsServerOptions == nil {
		log.Infof("mcp server options all nil, use default http options")
		cp.HttpServerOptions = &mcphttp.DefaultHttpServerOptions
	}

	if cp.HttpServerOptions != nil {
		if cp.HttpServerExtraOptions != nil {
			cp.HttpServerOptions.ApplyExtra(*cp.HttpServerExtraOptions)
		}
		return mcphttp.NewServer(cp.HttpServerOptions), nil
	} else if cp.XdsServerOptions != nil {
		return mcpxds.NewServer(cp.XdsServerOptions), nil
	} else {
		return nil, fmt.Errorf("no conf")
	}
}
