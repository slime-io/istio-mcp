package resource

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"istio.io/istio-mcp/pkg/util"
)

const (
	GroupGoogleApi   = "type.googleapis.com"
	KindK8sApiPrefix = "k8s.io.api"
)

var GroupK8sCore = "core"

type GroupVersionKind struct {
	Group   string `json:"group"`
	Version string `json:"version"`
	Kind    string `json:"kind"`
}

var (
	EmptyGvk     = GroupVersionKind{}
	AllGvk       = EmptyGvk
	AllNamespace = ""
)

var _ fmt.Stringer = GroupVersionKind{}

func (g GroupVersionKind) MarshalText() ([]byte, error) {
	return []byte(g.String()), nil
}

func (g GroupVersionKind) String() string {
	//if g.Group == "" {
	//	g.Group = "core"
	//}
	if g.Version == "" {
		return g.Group + "/" + g.Kind
	} else {
		return g.Group + "/" + g.Version + "/" + g.Kind
	}
}

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
	a, err := util.MessageToAny(msg)
	if err != nil {
		return "", err
	}
	return a.TypeUrl, nil
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

	return GroupVersionKind{"", tup[2], tup[3]}
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
