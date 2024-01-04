package internal

import (
	"strings"

	"github.com/zeromicro/go-zero/zrpc/resolver/internal/targets"
	"google.golang.org/grpc/resolver"
)

type directBuilder struct{}

func (d *directBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (
	resolver.Resolver, error) {
	var addrs []resolver.Address
	// 使用逗号拆分成多个
	endpoints := strings.FieldsFunc(targets.GetEndpoints(target), func(r rune) bool {
		return r == EndpointSepChar
	})

	// 打乱所有连接，取前32个
	for _, val := range subset(endpoints, subsetSize) {
		addrs = append(addrs, resolver.Address{
			Addr: val,
		})
	}
	if err := cc.UpdateState(resolver.State{
		Addresses: addrs,
	}); err != nil {
		return nil, err
	}

	return &nopResolver{cc: cc}, nil
}

func (d *directBuilder) Scheme() string {
	return DirectScheme
}
