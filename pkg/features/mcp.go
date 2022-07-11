package features

import (
	"strings"
	"time"

	"istio.io/pkg/env"
)

var (
	XdsSendTimeout = env.RegisterDurationVar(
		"XDS_SEND_TIMEOUT",
		5*time.Second,
		"",
	).Get()

	SentNonceRecordNum = env.RegisterIntVar(
		"SENT_NONCE_RECORD_NUM",
		1,
		"",
	).Get()

	// MaxRecvMsgSize The max receive buffer size of gRPC received channel of Pilot in bytes.
	MaxRecvMsgSize = env.RegisterIntVar(
		"MCP_XDS_GPRC_MAXRECVMSGSIZE",
		4*1024*1024,
		"Sets the max receive buffer size of gRPC stream in bytes.",
	).Get()

	McpIncPush = env.RegisterBoolVar(
		"MCP_INC_PUSH",
		false,
		"",
	).Get()

	StrictIstioRev = env.RegisterBoolVar(
		"STRICT_ISTIO_REV",
		false,
		"whether to apply strict rev-match which means "+
			"empty-rev istio will only see no-or-empty-rev configs and "+
			"non-empty-rev istio will only see non-empty rev configs",
	).Get()

	extraRevMap = env.RegisterStringVar(
		"EXTRA_REV_MAP",
		"",
		"rules to map config rev to new additional values. "+
			"ie. rev1:,rev2:rev21 will cause clients of rev1 will get configs of rev1 or no-rev and "+
			"clients of rev2 will get configs of rev2 and rev21."+
			"this feature is mainly used for downwards compatibility.",
	).Get()

	ExtraRevMap map[string]string
)

func initExtraRevMap() {
	ExtraRevMap = map[string]string{}
	for _, part := range strings.Split(extraRevMap, ",") {
		pair := strings.SplitN(part, ":", 2)
		if len(pair) != 2 {
			continue
		}

		k, v := strings.Trim(pair[0], " "), strings.Trim(pair[1], " ")
		ExtraRevMap[k] = v
	}
}

func init() {
	if extraRevMap != "" {
		initExtraRevMap()
	}
}
