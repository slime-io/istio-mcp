package resource

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"istio.io/libistio/pkg/config"
)

const (
	GroupGoogleApi   = "type.googleapis.com"
	KindK8sApiPrefix = "k8s.io.api"
)

var GroupK8sCore = "core"

type GroupVersionKind = config.GroupVersionKind

var (
	EmptyGvk     = GroupVersionKind{}
	AllGvk       = EmptyGvk
	AllNamespace = ""
)

var _ fmt.Stringer = GroupVersionKind{}

func TypeUrlToGvkTuple(typeUrl string) []string {
	return strings.SplitN(typeUrl, "/", 3)
}

func TypeUrlToGvk(typeUrl string) GroupVersionKind {
	if typeUrl == "" {
		return EmptyGvk
	}
	return TupleToGvk(TypeUrlToGvkTuple(typeUrl))
}

func TupleToGvk(t []string) GroupVersionKind {
	ret := GroupVersionKind{}
	l := len(t)
	if l > 0 {
		ret.Kind = t[l-1]
	}
	if l > 1 {
		ret.Group = t[0]
	}
	if l > 2 {
		ret.Version = t[1]
	}
	return ret
}

func MsgTypeUrl(msg proto.Message) (string, error) {
	any, err := anypb.New(msg)
	if err != nil {
		return "", err
	}
	return any.GetTypeUrl(), nil
}

func MsgGvk(msg proto.Message) (GroupVersionKind, error) {
	typeUrl, err := MsgTypeUrl(msg)
	if err != nil {
		return GroupVersionKind{}, err
	}
	return TypeUrlToGvk(typeUrl), nil
}

func MayRevertK8sGvk(gvk GroupVersionKind) GroupVersionKind {
	if gvk.Group != GroupGoogleApi || gvk.Version != "" || !strings.HasPrefix(gvk.Kind, KindK8sApiPrefix) {
		return gvk
	}

	tup := strings.Split(strings.TrimPrefix(gvk.Kind, KindK8sApiPrefix), ".")
	if len(tup) != 4 {
		return gvk
	}

	return GroupVersionKind{
		Group:   "",
		Version: tup[2],
		Kind:    tup[3],
	}
}

func ConvertShortK8sGvk(gvk GroupVersionKind) GroupVersionKind {
	if gvk.Group == "" {
		gvk.Group = GroupK8sCore
	}
	return GroupVersionKind{
		Group:   GroupGoogleApi,
		Version: "",
		Kind:    strings.Join([]string{KindK8sApiPrefix, gvk.Group, gvk.Version, gvk.Kind}, "."),
	}
}

func IsK8sShortGvk(gvk GroupVersionKind) bool {
	return (gvk.Group == "" && gvk != EmptyGvk) || gvk.Group == GroupK8sCore
}
