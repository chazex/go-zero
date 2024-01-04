package internal

import (
	"strings"

	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc/resolver/internal/targets"
	"google.golang.org/grpc/resolver"
)

type discovBuilder struct{}

// Build cc 是 ccResolverWrapper
func (b *discovBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (
	resolver.Resolver, error) {
	hosts := strings.FieldsFunc(targets.GetAuthority(target), func(r rune) bool {
		return r == EndpointSepChar
	})
	sub, err := discov.NewSubscriber(hosts, targets.GetEndpoints(target))
	if err != nil {
		return nil, err
	}

	update := func() {
		var addrs []resolver.Address
		for _, val := range subset(sub.Values(), subsetSize) {
			addrs = append(addrs, resolver.Address{
				Addr: val,
			})
		}
		if err := cc.UpdateState(resolver.State{
			Addresses: addrs,
		}); err != nil {
			logx.Error(err)
		}
	}
	// 在container中，添加一个listener. 这样etcd变化的时候，就会执行update方法, update方法会通过UpdateState通知grpc-go连接发生了变化。
	sub.AddListener(update)
	// 初始化的时候，先执行一下update()，用来触发cc.UpdateState()
	update()

	return &nopResolver{cc: cc}, nil
}

func (b *discovBuilder) Scheme() string {
	return DiscovScheme
}
