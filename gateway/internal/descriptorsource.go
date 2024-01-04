package internal

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/fullstorydev/grpcurl"
	"github.com/jhump/protoreflect/desc"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
)

type Method struct {
	HttpMethod string
	HttpPath   string
	RpcPath    string
}

// GetMethods returns all methods of the given grpcurl.DescriptorSource.
func GetMethods(source grpcurl.DescriptorSource) ([]Method, error) {
	// grpc的所有Service
	svcs, err := source.ListServices()
	if err != nil {
		return nil, err
	}

	var methods []Method
	for _, svc := range svcs {
		// 通过服务名，查找对应的服务描述对象 （source是serverSource）
		d, err := source.FindSymbol(svc)
		if err != nil {
			return nil, err
		}

		switch val := d.(type) {
		case *desc.ServiceDescriptor:
			// 某个service下的所有方法描述对象
			svcMethods := val.GetMethods()
			for _, method := range svcMethods {
				// 拼接rpc路由
				rpcPath := fmt.Sprintf("%s/%s", svc, method.GetName())
				// 获取 method 的http option 配置
				ext := proto.GetExtension(method.GetMethodOptions(), annotations.E_Http)
				if ext == nil {
					// 没有配置http的option
					methods = append(methods, Method{
						RpcPath: rpcPath,
					})
					continue
				}

				httpExt, ok := ext.(*annotations.HttpRule)
				if !ok {
					methods = append(methods, Method{
						RpcPath: rpcPath,
					})
					continue
				}

				// 依据proto文件中的http的的配置，生成Method
				switch rule := httpExt.GetPattern().(type) {
				case *annotations.HttpRule_Get:
					methods = append(methods, Method{
						HttpMethod: http.MethodGet,
						HttpPath:   adjustHttpPath(rule.Get),
						RpcPath:    rpcPath,
					})
				case *annotations.HttpRule_Post:
					methods = append(methods, Method{
						HttpMethod: http.MethodPost,
						HttpPath:   adjustHttpPath(rule.Post),
						RpcPath:    rpcPath,
					})
				case *annotations.HttpRule_Put:
					methods = append(methods, Method{
						HttpMethod: http.MethodPut,
						HttpPath:   adjustHttpPath(rule.Put),
						RpcPath:    rpcPath,
					})
				case *annotations.HttpRule_Delete:
					methods = append(methods, Method{
						HttpMethod: http.MethodDelete,
						HttpPath:   adjustHttpPath(rule.Delete),
						RpcPath:    rpcPath,
					})
				case *annotations.HttpRule_Patch:
					methods = append(methods, Method{
						HttpMethod: http.MethodPatch,
						HttpPath:   adjustHttpPath(rule.Patch),
						RpcPath:    rpcPath,
					})
				default:
					methods = append(methods, Method{
						RpcPath: rpcPath,
					})
				}
			}
		}
	}

	return methods, nil
}

// 适配路径中带参数的情况。  proto http option是{}形式 /a/b/{id}，而http的路由处理时a/b/:c这种形式
func adjustHttpPath(path string) string {
	path = strings.ReplaceAll(path, "{", ":")
	path = strings.ReplaceAll(path, "}", "")
	return path
}
