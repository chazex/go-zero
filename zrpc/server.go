package zrpc

import (
	"log"
	"time"

	"github.com/zeromicro/go-zero/core/load"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/zrpc/internal"
	"github.com/zeromicro/go-zero/zrpc/internal/auth"
	"github.com/zeromicro/go-zero/zrpc/internal/serverinterceptors"
	"google.golang.org/grpc"
)

// A RpcServer is a rpc server.
type RpcServer struct {
	server   internal.Server
	register internal.RegisterFn
}

// MustNewServer returns a RpcSever, exits on any error.
func MustNewServer(c RpcServerConf, register internal.RegisterFn) *RpcServer {
	server, err := NewServer(c, register)
	if err != nil {
		// 出错程序直接退出
		log.Fatal(err)
	}

	return server
}

// NewServer returns a RpcServer.
func NewServer(c RpcServerConf, register internal.RegisterFn) (*RpcServer, error) {
	var err error
	if err = c.Validate(); err != nil {
		return nil, err
	}

	var server internal.Server
	metrics := stat.NewMetrics(c.ListenOn)
	serverOptions := []internal.ServerOption{
		internal.WithMetrics(metrics),
		internal.WithRpcHealth(c.Health),
	}

	if c.HasEtcd() {
		// 使用etcd作为服务注册和发现
		server, err = internal.NewRpcPubServer(c.Etcd, c.ListenOn, serverOptions...)
		if err != nil {
			return nil, err
		}
	} else {
		// 没有服务注册和发现
		server = internal.NewRpcServer(c.ListenOn, serverOptions...)
	}

	server.SetName(c.Name)
	if err = setupInterceptors(server, c, metrics); err != nil {
		return nil, err
	}

	rpcServer := &RpcServer{
		server:   server,
		register: register,
	}

	// 启动日志、普罗米修斯、链路追踪
	if err = c.SetUp(); err != nil {
		return nil, err
	}

	return rpcServer, nil
}

// AddOptions adds given options.
func (rs *RpcServer) AddOptions(options ...grpc.ServerOption) {
	rs.server.AddOptions(options...)
}

// AddStreamInterceptors adds given stream interceptors.
func (rs *RpcServer) AddStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) {
	rs.server.AddStreamInterceptors(interceptors...)
}

// AddUnaryInterceptors adds given unary interceptors.
func (rs *RpcServer) AddUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) {
	rs.server.AddUnaryInterceptors(interceptors...)
}

// Start starts the RpcServer.
// Graceful shutdown is enabled by default.
// Use proc.SetTimeToForceQuit to customize the graceful shutdown period.
func (rs *RpcServer) Start() {
	// 实际调用的是rpcServer or keepAliveServer的Start()方法
	if err := rs.server.Start(rs.register); err != nil {
		logx.Error(err)
		panic(err)
	}
}

// Stop stops the RpcServer.
func (rs *RpcServer) Stop() {
	logx.Close()
}

// DontLogContentForMethod disable logging content for given method.
func DontLogContentForMethod(method string) {
	serverinterceptors.DontLogContentForMethod(method)
}

// SetServerSlowThreshold sets the slow threshold on server side.
func SetServerSlowThreshold(threshold time.Duration) {
	serverinterceptors.SetSlowThreshold(threshold)
}

func setupInterceptors(server internal.Server, c RpcServerConf, metrics *stat.Metrics) error {
	if c.CpuThreshold > 0 {
		shedder := load.NewAdaptiveShedder(load.WithCpuThreshold(c.CpuThreshold))
		server.AddUnaryInterceptors(serverinterceptors.UnarySheddingInterceptor(shedder, metrics))
	}

	if c.Timeout > 0 {
		// 添加请求超时拦截器
		server.AddUnaryInterceptors(serverinterceptors.UnaryTimeoutInterceptor(
			time.Duration(c.Timeout) * time.Millisecond))
	}

	if c.Auth {
		authenticator, err := auth.NewAuthenticator(c.Redis.NewRedis(), c.Redis.Key, c.StrictControl)
		if err != nil {
			return err
		}

		// 权限验证拦截器
		server.AddStreamInterceptors(serverinterceptors.StreamAuthorizeInterceptor(authenticator))
		server.AddUnaryInterceptors(serverinterceptors.UnaryAuthorizeInterceptor(authenticator))
	}

	return nil
}
