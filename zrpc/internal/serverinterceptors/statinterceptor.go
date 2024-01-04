package serverinterceptors

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/lang"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/syncx"
	"github.com/zeromicro/go-zero/core/timex"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

const defaultSlowThreshold = time.Millisecond * 500

var (
	// 用于存储，在grpc请求中，哪些方法不要记录请求的body（比如body可能很大，或者是二进制的）
	notLoggingContentMethods sync.Map
	slowThreshold            = syncx.ForAtomicDuration(defaultSlowThreshold)
)

// DontLogContentForMethod disable logging content for given method.
func DontLogContentForMethod(method string) {
	notLoggingContentMethods.Store(method, lang.Placeholder)
}

// SetSlowThreshold 这是请求响应时间阈值，用来记录slowcall日志
// SetSlowThreshold sets the slow threshold.
func SetSlowThreshold(threshold time.Duration) {
	slowThreshold.Set(threshold)
}

// UnaryStatInterceptor returns a func that uses given metrics to report stats.
func UnaryStatInterceptor(metrics *stat.Metrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (resp interface{}, err error) {
		startTime := timex.Now()
		defer func() {
			duration := timex.Since(startTime)
			metrics.Add(stat.Task{
				Duration: duration,
			})
			logDuration(ctx, info.FullMethod, req, duration)
		}()

		return handler(ctx, req)
	}
}

func logDuration(ctx context.Context, method string, req interface{}, duration time.Duration) {
	var addr string
	client, ok := peer.FromContext(ctx)
	if ok {
		addr = client.Addr.String()
	}

	logger := logx.WithContext(ctx).WithDuration(duration)
	// 判断方法是否需要记录请求体
	_, ok = notLoggingContentMethods.Load(method)
	if ok {
		if duration > slowThreshold.Load() {
			logger.Slowf("[RPC] slowcall - %s - %s", addr, method)
		} else {
			logger.Infof("%s - %s", addr, method)
		}
	} else {
		// 对请求体做序列化
		content, err := json.Marshal(req)
		if err != nil {
			// 序列化失败
			logx.WithContext(ctx).Errorf("%s - %s", addr, err.Error())
		} else if duration > slowThreshold.Load() {
			logger.Slowf("[RPC] slowcall - %s - %s - %s", addr, method, string(content))
		} else {
			logger.Infof("%s - %s - %s", addr, method, string(content))
		}
	}
}
