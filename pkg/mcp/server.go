package mcp

import (
	"context"
	"istio.io/istio-mcp/pkg/config/schema/resource"

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

type ConfigPushStatus struct {
	PushVer, AckVer string
}

type ClientDetailedInfo struct {
	ClientInfo
	PushStatus map[resource.GroupVersionKind]ConfigPushStatus
}

type ClientEvent struct {
	Clients   []ClientInfo
	EventType ClientEventType
}

type PushRequest struct {
	RevChangeConfigs map[model.ConfigKey]struct{}
}

type Server interface {
	ClientsInfo() map[string]ClientDetailedInfo
	RegisterClientEventHandler(h func(ClientEvent))
	SetConfigStore(store model.ConfigStore)
	NotifyPush(req *PushRequest)
	Start(ctx context.Context)
}
