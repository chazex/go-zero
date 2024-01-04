package serverinterceptors

import (
	"context"
	"github.com/zeromicro/go-zero/core/metric"
	"github.com/zeromicro/go-zero/core/timex"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"strconv"
)

// 用于rpc请求的Prometheus指标收集中间件。

const serverNamespace = "rpc_server"

var (
	metricServerReqDur = metric.NewHistogramVec(&metric.HistogramVecOpts{
		Namespace: serverNamespace,
		Subsystem: "requests",
		Name:      "duration_ms",
		Help:      "rpc server requests duration(ms).",
		Labels:    []string{"method"},
		Buckets:   []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000},
	})

	metricServerReqCodeTotal = metric.NewCounterVec(&metric.CounterVecOpts{
		Namespace: serverNamespace,
		Subsystem: "requests",
		Name:      "code_total",
		Help:      "rpc server requests code count.",
		Labels:    []string{"method", "code"},
	})
)

// UnaryPrometheusInterceptor reports the statistics to the prometheus server.
func UnaryPrometheusInterceptor(ctx context.Context, req any,
	info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	startTime := timex.Now()
	resp, err := handler(ctx, req)
	// 请求方法的耗时
	metricServerReqDur.Observe(timex.Since(startTime).Milliseconds(), info.FullMethod)
	// 请求方法的rpc状态码
	metricServerReqCodeTotal.Inc(info.FullMethod, strconv.Itoa(int(status.Code(err))))
	return resp, err
}
