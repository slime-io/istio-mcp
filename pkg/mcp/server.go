package mcp

import (
	"context"

	"istio.io/istio-mcp/pkg/model"
)

type ClientEventType int

const (
	ClientEventAdd = iota
	ClientEventDelete
)

type ClientInfo struct {
	ClientId string // optional. then addr will be used as identifier
	ConnId   string // as id
	Addr     string // ip:port. mandatory
}

type ClientEvent struct {
	Clients   []ClientInfo
	EventType ClientEventType
}

type Server interface {
	RegisterClientEventHandler(h func(ClientEvent))
	SetConfigStore(store model.ConfigStore)
	NotifyPush()
	Start(ctx context.Context)
}
