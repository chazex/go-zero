package gateway

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/fullstorydev/grpcurl"
	"github.com/golang/protobuf/jsonpb"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/mr"
	"github.com/zeromicro/go-zero/gateway/internal"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/rest/httpx"
	"github.com/zeromicro/go-zero/zrpc"
	"google.golang.org/grpc/codes"
)

type (
	// Server is a gateway server.
	Server struct {
		*rest.Server
		upstreams     []Upstream
		processHeader func(http.Header) []string
		dialer        func(conf zrpc.RpcClientConf) zrpc.Client
	}

	// Option defines the method to customize Server.
	Option func(svr *Server)
)

// MustNewServer creates a new gateway server.
func MustNewServer(c GatewayConf, opts ...Option) *Server {
	svr := &Server{
		upstreams: c.Upstreams,
		Server:    rest.MustNewServer(c.RestConf),
	}
	for _, opt := range opts {
		opt(svr)
	}

	return svr
}

// Start starts the gateway server.
func (s *Server) Start() {
	logx.Must(s.build())
	// 启动http服务
	s.Server.Start()
}

// Stop stops the gateway server.
func (s *Server) Stop() {
	s.Server.Stop()
}

func (s *Server) build() error {
	if err := s.ensureUpstreamNames(); err != nil {
		return err
	}

	return mr.MapReduceVoid(func(source chan<- Upstream) {
		for _, up := range s.upstreams {
			source <- up
		}
	}, func(up Upstream, writer mr.Writer[rest.Route], cancel func(error)) {
		var cli zrpc.Client
		if s.dialer != nil {
			cli = s.dialer(up.Grpc)
		} else {
			// 创建rpc client, 内部会做原生rpc dial
			cli = zrpc.MustNewClient(up.Grpc)
		}
		// 请求grpc反射服务，或者基于proto文件
		source, err := s.createDescriptorSource(cli, up)
		if err != nil {
			cancel(fmt.Errorf("%s: %w", up.Name, err))
			return
		}

		// 依据grpc的服务和方法，转换为对应的http
		methods, err := internal.GetMethods(source)
		if err != nil {
			cancel(fmt.Errorf("%s: %w", up.Name, err))
			return
		}

		resolver := grpcurl.AnyResolverFromDescriptorSource(source)
		for _, m := range methods {
			// 对于在proto文件中配置了，http method 和path的grpc服务，构建http路由
			if len(m.HttpMethod) > 0 && len(m.HttpPath) > 0 {
				writer.Write(rest.Route{
					Method:  m.HttpMethod,
					Path:    m.HttpPath,
					Handler: s.buildHandler(source, resolver, cli, m.RpcPath),
				})
			}
		}

		// 下面这块代码的作用：　如果我们不想在proto文件中配置http的option，go-zero支持在配置文件中配置 http_method，http_path, grpc_path的映射关系
		methodSet := make(map[string]struct{})
		for _, m := range methods {
			methodSet[m.RpcPath] = struct{}{}
		}

		for _, m := range up.Mappings {
			if _, ok := methodSet[m.RpcPath]; !ok {
				cancel(fmt.Errorf("%s: rpc method %s not found", up.Name, m.RpcPath))
				return
			}

			writer.Write(rest.Route{
				Method:  strings.ToUpper(m.Method),
				Path:    m.Path,
				Handler: s.buildHandler(source, resolver, cli, m.RpcPath),
			})
		}
	}, func(pipe <-chan rest.Route, cancel func(error)) {
		for route := range pipe {
			// 添加到http的路由中
			s.Server.AddRoute(route)
		}
	})
}

// 构建http handler
func (s *Server) buildHandler(source grpcurl.DescriptorSource, resolver jsonpb.AnyResolver,
	cli zrpc.Client, rpcPath string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// 参数解析
		parser, err := internal.NewRequestParser(r, resolver)
		if err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
			return
		}
		w.Header().Set(httpx.ContentType, httpx.JsonContentType)
		handler := internal.NewEventHandler(w, resolver)
		// 通过grpcurl来调用grpc
		if err := grpcurl.InvokeRPC(r.Context(), source, cli.Conn(), rpcPath, s.prepareMetadata(r.Header),

			// 这个timeout是配置文件中的 GatewayConf.Timeout
			// 可以被 http request 的 Grpc-Timeout 所覆盖。
			//timeout := internal.GetTimeout(r.Header, s.timeout)
			//ctx, can := context.WithTimeout(r.Context(), timeout)
			//defer can()
			//
			//w.Header().Set(httpx.ContentType, httpx.JsonContentType)
			//handler := internal.NewEventHandler(w, resolver)
			//
			//if err := grpcurl.InvokeRPC(ctx, source, cli.Conn(), rpcPath, s.prepareMetadata(r.Header),
			handler, parser.Next); err != nil {
			httpx.ErrorCtx(r.Context(), w, err)
		}

		st := handler.Status
		if st.Code() != codes.OK {
			httpx.ErrorCtx(r.Context(), w, st.Err())
		}
	}
}

func (s *Server) createDescriptorSource(cli zrpc.Client, up Upstream) (grpcurl.DescriptorSource, error) {
	var source grpcurl.DescriptorSource
	var err error

	if len(up.ProtoSets) > 0 {
		source, err = grpcurl.DescriptorSourceFromProtoSets(up.ProtoSets...)
		if err != nil {
			return nil, err
		}
	} else {
		// 动态获取反射（访问grpc server的反射服务）
		//refCli := grpc_reflection_v1alpha.NewServerReflectionClient(cli.Conn())
		//// grpc原生也可以获取服务/方法列表，https://juejin.cn/s/golang%20grpc%20%E5%8F%8D%E5%B0%84
		//// 这里使用了grpcurl来做的（它内部又简介用了一个老外的库），本质还是通过grpc原生来做的。
		//client := grpcreflect.NewClient(context.Background(), refCli)

		client := grpcreflect.NewClientAuto(context.Background(), cli.Conn())
		source = grpcurl.DescriptorSourceFromServer(context.Background(), client)
	}

	return source, nil
}

func (s *Server) ensureUpstreamNames() error {
	for i := 0; i < len(s.upstreams); i++ {
		target, err := s.upstreams[i].Grpc.BuildTarget()
		if err != nil {
			return err
		}

		s.upstreams[i].Name = target
	}

	return nil
}

func (s *Server) prepareMetadata(header http.Header) []string {
	vals := internal.ProcessHeaders(header)
	if s.processHeader != nil {
		vals = append(vals, s.processHeader(header)...)
	}

	return vals
}

// WithHeaderProcessor sets a processor to process request headers.
// The returned headers are used as metadata to invoke the RPC.
func WithHeaderProcessor(processHeader func(http.Header) []string) func(*Server) {
	return func(s *Server) {
		s.processHeader = processHeader
	}
}

// withDialer sets a dialer to create a gRPC client.
func withDialer(dialer func(conf zrpc.RpcClientConf) zrpc.Client) func(*Server) {
	return func(s *Server) {
		s.dialer = dialer
	}
}
