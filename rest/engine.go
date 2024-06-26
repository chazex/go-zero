package rest

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/zeromicro/go-zero/core/codec"
	"github.com/zeromicro/go-zero/core/load"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/rest/chain"
	"github.com/zeromicro/go-zero/rest/handler"
	"github.com/zeromicro/go-zero/rest/httpx"
	"github.com/zeromicro/go-zero/rest/internal"
	"github.com/zeromicro/go-zero/rest/internal/response"
)

// use 1000m to represent 100%
const topCpuUsage = 1000

// ErrSignatureConfig is an error that indicates bad config for signature.
var ErrSignatureConfig = errors.New("bad config for Signature")

type engine struct {
	conf RestConf
	//  将grpc所对应的http路由添加到了这里。 代码模板的路由也添加到这里。
	routes []featuredRoutes
	// timeout is the max timeout of all routes
	timeout              time.Duration
	unauthorizedCallback handler.UnauthorizedCallback
	unsignedCallback     handler.UnsignedCallback
	chain                chain.Chain
	middlewares          []Middleware
	shedder              load.Shedder
	priorityShedder      load.Shedder
	tlsConfig            *tls.Config
}

func newEngine(c RestConf) *engine {
	svr := &engine{
		conf:    c,
		timeout: time.Duration(c.Timeout) * time.Millisecond,
	}

	if c.CpuThreshold > 0 {
		svr.shedder = load.NewAdaptiveShedder(load.WithCpuThreshold(c.CpuThreshold))
		svr.priorityShedder = load.NewAdaptiveShedder(load.WithCpuThreshold(
			(c.CpuThreshold + topCpuUsage) >> 1))
	}

	return svr
}

func (ng *engine) addRoutes(r featuredRoutes) {
	ng.routes = append(ng.routes, r)

	// need to guarantee the timeout is the max of all routes
	// otherwise impossible to set http.Server.ReadTimeout & WriteTimeout
	if r.timeout > ng.timeout {
		ng.timeout = r.timeout
	}
}

func (ng *engine) appendAuthHandler(fr featuredRoutes, chn chain.Chain,
	verifier func(chain.Chain) chain.Chain) chain.Chain {
	if fr.jwt.enabled {
		if len(fr.jwt.prevSecret) == 0 {
			chn = chn.Append(handler.Authorize(fr.jwt.secret,
				handler.WithUnauthorizedCallback(ng.unauthorizedCallback)))
		} else {
			chn = chn.Append(handler.Authorize(fr.jwt.secret,
				handler.WithPrevSecret(fr.jwt.prevSecret),
				handler.WithUnauthorizedCallback(ng.unauthorizedCallback)))
		}
	}

	return verifier(chn)
}

func (ng *engine) bindFeaturedRoutes(router httpx.Router, fr featuredRoutes, metrics *stat.Metrics) error {
	// 调用 signatureVerifier 方法，根据 fr.signature 的值创建一个签名验证器
	verifier, err := ng.signatureVerifier(fr.signature)
	if err != nil {
		return err
	}

	for _, route := range fr.routes {
		// 每一个路由，都绑定了一堆中间件。
		// 这样做的好处是，中间件可以做到基于路由维度的配置。比如不同的路由，有不同的超时时间，不同的熔断策略等等。
		if err := ng.bindRoute(fr, router, metrics, route, verifier); err != nil {
			return err
		}
	}

	return nil
}

func (ng *engine) bindRoute(fr featuredRoutes, router httpx.Router, metrics *stat.Metrics,
	route Route, verifier func(chain.Chain) chain.Chain) error {
	chn := ng.chain
	if chn == nil {
		// 如果为空, 调用 buildChainWithNativeMiddlewares()根据 fr, route, metrics 构建一个 chain 对象
		chn = ng.buildChainWithNativeMiddlewares(fr, route, metrics)
	}

	// 添加权限中间件
	chn = ng.appendAuthHandler(fr, chn, verifier)

	for _, middleware := range ng.middlewares {
		chn = chn.Append(convertMiddleware(middleware))
	}
	// 构建出洋葱芯
	handle := chn.ThenFunc(route.Handler)

	// 根据Method， Path， Handler挂载到路由树上（这里的router，就是server.router，实际类型是patRouter，实现了httpx.Router， 这个router是最终）
	return router.Handle(route.Method, route.Path, handle)
}

func (ng *engine) bindRoutes(router httpx.Router) error {
	// 配置指标统计： (%s) - qps: %.1f/s, drops: %d, avg time: %.1fms, med: %.1fms, " 90th: %.1fms, 99th: %.1fms, 99.9th: %.1fms"
	metrics := ng.createMetrics()

	// 为特性路由（grpc 转换成的 http路由）添加中间件？
	for _, fr := range ng.routes {
		if err := ng.bindFeaturedRoutes(router, fr, metrics); err != nil {
			return err
		}
	}

	return nil
}

func (ng *engine) buildChainWithNativeMiddlewares(fr featuredRoutes, route Route,
	metrics *stat.Metrics) chain.Chain {
	chn := chain.New()

	if ng.conf.Middlewares.Trace {
		chn = chn.Append(handler.TraceHandler(ng.conf.Name,
			route.Path,
			handler.WithTraceIgnorePaths(ng.conf.TraceIgnorePaths)))
	}
	if ng.conf.Middlewares.Log {
		chn = chn.Append(ng.getLogHandler())
	}
	if ng.conf.Middlewares.Prometheus {
		chn = chn.Append(handler.PrometheusHandler(route.Path, route.Method))
	}
	if ng.conf.Middlewares.MaxConns {
		chn = chn.Append(handler.MaxConnsHandler(ng.conf.MaxConns))
	}
	if ng.conf.Middlewares.Breaker {
		chn = chn.Append(handler.BreakerHandler(route.Method, route.Path, metrics))
	}
	if ng.conf.Middlewares.Shedding {
		chn = chn.Append(handler.SheddingHandler(ng.getShedder(fr.priority), metrics))
	}
	if ng.conf.Middlewares.Timeout {
		chn = chn.Append(handler.TimeoutHandler(ng.checkedTimeout(fr.timeout)))
	}
	if ng.conf.Middlewares.Recover {
		chn = chn.Append(handler.RecoverHandler)
	}
	if ng.conf.Middlewares.Metrics {
		chn = chn.Append(handler.MetricHandler(metrics))
	}
	if ng.conf.Middlewares.MaxBytes {
		chn = chn.Append(handler.MaxBytesHandler(ng.checkedMaxBytes(fr.maxBytes)))
	}
	if ng.conf.Middlewares.Gunzip {
		chn = chn.Append(handler.GunzipHandler)
	}

	return chn
}

func (ng *engine) checkedMaxBytes(bytes int64) int64 {
	if bytes > 0 {
		return bytes
	}

	return ng.conf.MaxBytes
}

func (ng *engine) checkedTimeout(timeout time.Duration) time.Duration {
	if timeout > 0 {
		return timeout
	}

	return time.Duration(ng.conf.Timeout) * time.Millisecond
}

func (ng *engine) createMetrics() *stat.Metrics {
	var metrics *stat.Metrics

	// 配置指标统计： (%s) - qps: %.1f/s, drops: %d, avg time: %.1fms, med: %.1fms, " 90th: %.1fms, 99th: %.1fms, 99.9th: %.1fms"
	if len(ng.conf.Name) > 0 {
		metrics = stat.NewMetrics(ng.conf.Name)
	} else {
		metrics = stat.NewMetrics(fmt.Sprintf("%s:%d", ng.conf.Host, ng.conf.Port))
	}

	return metrics
}

func (ng *engine) getLogHandler() func(http.Handler) http.Handler {
	if ng.conf.Verbose {
		return handler.DetailedLogHandler
	}

	return handler.LogHandler
}

func (ng *engine) getShedder(priority bool) load.Shedder {
	if priority && ng.priorityShedder != nil {
		return ng.priorityShedder
	}

	return ng.shedder
}

// notFoundHandler returns a middleware that handles 404 not found requests.
func (ng *engine) notFoundHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chn := chain.New(
			handler.TraceHandler(ng.conf.Name,
				"",
				handler.WithTraceIgnorePaths(ng.conf.TraceIgnorePaths)),
			ng.getLogHandler(),
		)

		var h http.Handler
		if next != nil {
			h = chn.Then(next)
		} else {
			h = chn.Then(http.NotFoundHandler())
		}

		cw := response.NewHeaderOnceResponseWriter(w)
		h.ServeHTTP(cw, r)
		cw.WriteHeader(http.StatusNotFound)
	})
}

func (ng *engine) print() {
	var routes []string

	for _, fr := range ng.routes {
		for _, route := range fr.routes {
			routes = append(routes, fmt.Sprintf("%s %s", route.Method, route.Path))
		}
	}

	sort.Strings(routes)

	fmt.Println("Routes:")
	for _, route := range routes {
		fmt.Printf("  %s\n", route)
	}
}

func (ng *engine) setTlsConfig(cfg *tls.Config) {
	ng.tlsConfig = cfg
}

func (ng *engine) setUnauthorizedCallback(callback handler.UnauthorizedCallback) {
	ng.unauthorizedCallback = callback
}

func (ng *engine) setUnsignedCallback(callback handler.UnsignedCallback) {
	ng.unsignedCallback = callback
}

func (ng *engine) signatureVerifier(signature signatureSetting) (func(chain.Chain) chain.Chain, error) {
	if !signature.enabled {
		return func(chn chain.Chain) chain.Chain {
			return chn
		}, nil
	}

	if len(signature.PrivateKeys) == 0 {
		if signature.Strict {
			return nil, ErrSignatureConfig
		}

		return func(chn chain.Chain) chain.Chain {
			return chn
		}, nil
	}

	decrypters := make(map[string]codec.RsaDecrypter)
	for _, key := range signature.PrivateKeys {
		fingerprint := key.Fingerprint
		file := key.KeyFile
		decrypter, err := codec.NewRsaDecrypter(file)
		if err != nil {
			return nil, err
		}

		decrypters[fingerprint] = decrypter
	}

	return func(chn chain.Chain) chain.Chain {
		if ng.unsignedCallback == nil {
			return chn.Append(handler.LimitContentSecurityHandler(ng.conf.MaxBytes,
				decrypters, signature.Expiry, signature.Strict))
		}

		return chn.Append(handler.LimitContentSecurityHandler(ng.conf.MaxBytes,
			decrypters, signature.Expiry, signature.Strict, ng.unsignedCallback))
	}, nil
}

func (ng *engine) start(router httpx.Router, opts ...StartOption) error {
	if err := ng.bindRoutes(router); err != nil {
		return err
	}

	// 将 ng.withTimeout 方法作为一个 StartOption 参数添加到 opts 切片的开头
	// make sure user defined options overwrite default options
	opts = append([]StartOption{ng.withTimeout()}, opts...)

	// 如果配置文件中没有证书文件和密钥文件，则调用 internal 包中的 StartHttp 方法，启动一个 http 服务
	if len(ng.conf.CertFile) == 0 && len(ng.conf.KeyFile) == 0 {
		// 真实启动HTTP服务
		return internal.StartHttp(ng.conf.Host, ng.conf.Port, router, opts...)
	}

	// // 如果配置文件中有证书文件和密钥文件，则创建一个匿名函数，将 ng 的 tlsConfig 属性赋值给 svr 的 TLSConfig 属性，然后将这个匿名函数作为一个 StartOption 参数添加到 opts 切片的末尾
	// make sure user defined options overwrite default options
	opts = append([]StartOption{
		func(svr *http.Server) {
			if ng.tlsConfig != nil {
				svr.TLSConfig = ng.tlsConfig
			}
		},
	}, opts...)

	// 调用 internal 包中的 StartHttps 方法，启动一个 https 服务
	return internal.StartHttps(ng.conf.Host, ng.conf.Port, ng.conf.CertFile,
		ng.conf.KeyFile, router, opts...)
}

func (ng *engine) use(middleware Middleware) {
	ng.middlewares = append(ng.middlewares, middleware)
}

func (ng *engine) withTimeout() internal.StartOption {
	return func(svr *http.Server) {
		timeout := ng.timeout
		if timeout > 0 {
			// factor 0.8，避免clients发送的content-length比实际content长，如果没有这个timeout设置，server会超时响应503 Service Unavailable，触发熔断器
			// factor 0.8, to avoid clients send longer content-length than the actual content,
			// without this timeout setting, the server will time out and respond 503 Service Unavailable,
			// which triggers the circuit breaker.
			//svr.ReadTimeout = 4 * time.Duration(timeout) * time.Millisecond / 5
			//// factor 0.9，为了避免没有这个超时设置的客户端无法读取响应，服务端会超时响应503 Service Unavailable，从而触发熔断器。
			//// factor 0.9, to avoid clients not reading the response
			//// without this timeout setting, the server will time out and respond 503 Service Unavailable,
			//// which triggers the circuit breaker.
			//svr.WriteTimeout = 9 * time.Duration(timeout) * time.Millisecond / 10

			svr.ReadTimeout = 4 * timeout / 5
			// factor 1.1, to avoid servers don't have enough time to write responses.
			// setting the factor less than 1.0 may lead clients not receiving the responses.
			svr.WriteTimeout = 11 * timeout / 10
		}
	}
}

func convertMiddleware(ware Middleware) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return ware(next.ServeHTTP)
	}
}
