//go:build !linux && !android

package proxy

import (
	"context"

	"github.com/xtls/xray-core/common/net"
	"golang.org/x/time/rate"
)

func copyRawConnWithSplice(
	_ context.Context,
	_ net.Conn,
	_ net.Conn,
	_ *rate.Limiter,
	_ func(),
) (int64, bool, error) {
	return 0, false, nil
}
