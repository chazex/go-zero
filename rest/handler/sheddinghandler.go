package handler

import (
	"net/http"
	"sync"

	"github.com/zeromicro/go-zero/core/load"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/rest/httpx"
	"github.com/zeromicro/go-zero/rest/internal/response"
)

const serviceType = "api"

var (
	//统计器， 用于统计 total（请求总数）， pass（请求通过）， drop（请求被丢弃）的数量，它内部会每1分钟会对上面的统计清零，并打印日志
	sheddingStat *load.SheddingStat
	lock         sync.Mutex
)

// 过载保护中间件

// SheddingHandler returns a middleware that does load shedding.
func SheddingHandler(shedder load.Shedder, metrics *stat.Metrics) func(http.Handler) http.Handler {
	if shedder == nil {
		return func(next http.Handler) http.Handler {
			return next
		}
	}

	ensureSheddingStat()

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// 统计总量
			sheddingStat.IncrementTotal()
			// 检查是否被降载
			promise, err := shedder.Allow()
			if err != nil {
				// err!=nil, 执行降载，记录相关日志与指标
				metrics.AddDrop()
				sheddingStat.IncrementDrop()
				logx.Errorf("[http] dropped, %s - %s - %s",
					r.RequestURI, httpx.GetRemoteAddr(r), r.UserAgent())
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}

			cw := response.NewWithCodeResponseWriter(w)
			// 最后回调执行结果
			defer func() {
				// 执行失败
				if cw.Code == http.StatusServiceUnavailable {
					promise.Fail()
				} else {
					// 执行成功
					sheddingStat.IncrementPass()
					promise.Pass()
				}
			}()
			// 执行业务方法
			next.ServeHTTP(cw, r)
		})
	}
}

func ensureSheddingStat() {
	lock.Lock()
	if sheddingStat == nil {
		sheddingStat = load.NewSheddingStat(serviceType)
	}
	lock.Unlock()
}
