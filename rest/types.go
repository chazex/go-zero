package rest

import (
	"net/http"
	"time"
)

type (
	// Middleware defines the middleware method.
	Middleware func(next http.HandlerFunc) http.HandlerFunc

	// A Route is a http route.
	Route struct {
		//请求方法，比如GET、POST、PUT等
		Method string
		//请求路径，可以包含模式匹配的参数，比如/user/:id
		Path string
		//请求的处理函数,用于处理匹配到该路由的请求，并返回响应
		Handler http.HandlerFunc
	}

	// RouteOption defines the method to customize a featured route.
	RouteOption func(r *featuredRoutes)

	jwtSetting struct {
		enabled    bool
		secret     string
		prevSecret string
	}

	signatureSetting struct {
		SignatureConf
		enabled bool
	}

	// 有特征的路由， 就是为路由配置了很多的特性
	featuredRoutes struct {
		//表示这组路由的超时时间，如果请求处理超过这个时间，会返回超时错误
		timeout time.Duration
		//表示这组路由是否具有优先级，如果为true，这组路由会使用优先级限流器进行限流，否则使用普通限流器
		priority bool
		//包含了jwt验证相关的配置信息，比如是否开启jwt验证、密钥等
		jwt jwtSetting
		//包含了签名验证相关的配置信息，比如是否开启签名验证、签名算法等
		signature signatureSetting
		//用于存储这组路由的具体信息，每个Route包含了请求方法、路径和处理函数
		routes []Route
		//表示这组路由允许的最大请求体大小，如果请求体超过这个大小，会返回错误
		maxBytes int64
	}
)
