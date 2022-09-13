//go:generate mockgen -package internal -destination etcdclient_mock.go -source etcdclient.go EtcdClient

package internal

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// go-zero自己设计了一个etcd相关的接口， 原生的etcd client 已经实现了这个接口(这个接口，相当于从原生的etcd client中抽取出一部分方法出来形成的)

// EtcdClient interface represents an etcd client.
type EtcdClient interface {
	ActiveConnection() *grpc.ClientConn
	Close() error
	Ctx() context.Context
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	// Grant 获取一个lease
	Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
	// KeepAlive 尝试一直续约lease
	KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error)
	// Put 存储key val到etcd集群
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	// Revoke 废弃lease
	Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error)
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
}
