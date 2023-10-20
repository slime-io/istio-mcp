package util

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// MessageToAny converts from proto message to proto Any
func MessageToAny(msg proto.Message) (*anypb.Any, error) {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return &anypb.Any{
		// nolint: staticcheck
		TypeUrl: "type.googleapis.com/" + string(msg.ProtoReflect().Descriptor().FullName()),
		Value:   b,
	}, nil
}
