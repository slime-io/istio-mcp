package xds

import (
	"context"
	"testing"

	"gopkg.in/yaml.v2"

	"istio.io/api/networking/v1alpha3"

	"istio.io/istio-mcp/pkg/model"
)

var yamlData = `configmeta:
groupversionkind:
  group: networking.istio.io
  version: v1alpha3
  kind: ServiceEntry
name: group2
namespace: istio-system
domain: ""
labels: {}
annotations: {}
resourceversion: ""
creationtimestamp: 0001-01-01T00:00:00Z
`

var seData = `
hosts:
- group2.istio-system
addresses: []
ports:
- number: 80
  protocol: HTTP
  name: HTTP
  targetport: 0
location: 0
resolution: 1
endpoints:
- address: 192.168.0.2
  ports:
    HTTP: 8001
  labels: {}
  network: ""
  locality: ""
  weight: 1
  serviceaccount: ""
- address: 192.168.0.2
  ports:
    HTTP: 8000
  labels: {}
  network: ""
  locality: ""
  weight: 1
  serviceaccount: ""
workloadselector: null
exportto: []
subjectaltnames: []
inboundendpoints: []
dubboservicemode: ""
`

func TestServer(t *testing.T) {
	options := ServerOptions{}
	options.Port = 16010
	svr := NewServer(&options)
	store := model.NewSimpleConfigStore()
	svr.SetConfigStore(store)
	ctx := context.Background()
	svr.Start(ctx)

	const defaultNs = "istio-system"

	var se v1alpha3.ServiceEntry
	if err := yaml.Unmarshal([]byte(seData), &se); err != nil {
		t.Fatal(err)
	}
	var c model.Config
	if err := yaml.Unmarshal([]byte(yamlData), &c); err != nil {
		t.Fatal(err)
	}

	ns := c.Namespace
	if ns == "" {
		ns = defaultNs
	}

	c.Spec = &se
	store.Update(ns, model.MakeSimpleConfigSnapshot("1", []model.Config{
		c,
	}))

	// svr.NotifyPush()

	<-ctx.Done()
}
