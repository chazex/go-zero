package prometheus

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/syncx"
	"github.com/zeromicro/go-zero/core/threading"
)

var (
	once    sync.Once
	enabled syncx.AtomicBool
)

// Enabled returns if prometheus is enabled.
func Enabled() bool {
	return enabled.True()
}

// Enable enables prometheus.
func Enable() {
	// 设置全局开关为true
	enabled.Set(true)
}

// StartAgent starts a prometheus agent.
func StartAgent(c Config) {
	if len(c.Host) == 0 {
		return
	}

	// 为prometheus指标收集，专门开一个http服务
	// 在go-zero\zrpc\internal\serverinterceptors\prometheusinterceptor.go中,通过常量的方式进行的prometheus的声明和注册,然后在interceptor中使用UnaryPrometheusInterceptor来做的拦截器并统计指标。
	once.Do(func() {
		enabled.Set(true)
		threading.GoSafe(func() {
			http.Handle(c.Path, promhttp.Handler())
			addr := fmt.Sprintf("%s:%d", c.Host, c.Port)
			logx.Infof("Starting prometheus agent at %s", addr)
			// handler 为nil， 就会使用DefaultServeMux
			if err := http.ListenAndServe(addr, nil); err != nil {
				logx.Error(err)
			}
		})
	})
}
