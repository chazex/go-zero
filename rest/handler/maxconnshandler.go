package handler

import (
	"net/http"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/syncx"
	"github.com/zeromicro/go-zero/rest/internal"
)

// MaxConnsHandler returns a middleware that limit the concurrent connections.
func MaxConnsHandler(n int) func(http.Handler) http.Handler {
	if n <= 0 {
		return func(next http.Handler) http.Handler {
			return next
		}
	}

	return func(next http.Handler) http.Handler {
		latch := syncx.NewLimit(n)
		// 用于限制此服务 同时 可以处理的请求数， 超过此数，返回服务不可用
		// 注意这个和限流不太一样： 限流是做qps的限制，比如每秒的请求数不应该超过300。 我们把时间窗口设置为1s，只要在窗口内的请求数不超过300即可，他是一段时间范围内的统计。
		// 当然这种qps的限制可以基于整个服务来做，也可以基于某个用户来做
		// 最大并发是，某一时刻服务在处理请求的数量。如果我们的最大并发数设置为300， 但是我们每个请求的速度极快，比如20ms就可以处理一个请求，那么1s内，我们极限达到的qps就是 (1000/20) * 500
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// 尝试拿token
			if latch.TryBorrow() {
				// 拿到token
				defer func() {
					// 返还token
					if err := latch.Return(); err != nil {
						logx.Error(err)
					}
				}()

				next.ServeHTTP(w, r)
			} else {
				// 没拿到token， 同时处理的请求过多
				internal.Errorf(r, "concurrent connections over %d, rejected with code %d",
					n, http.StatusServiceUnavailable)
				w.WriteHeader(http.StatusServiceUnavailable)
			}
		})
	}
}
