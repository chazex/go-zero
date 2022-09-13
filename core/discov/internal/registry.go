package internal

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/contextx"
	"github.com/zeromicro/go-zero/core/lang"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/syncx"
	"github.com/zeromicro/go-zero/core/threading"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	registry = Registry{
		clusters: make(map[string]*cluster),
	}
	connManager = syncx.NewResourceManager()
)

// A Registry is a registry that manages the etcd client connections.
type Registry struct {
	//key是etcd的hosts列表排序值，值是hosts对应的cluster.以此来达到单例cluster
	clusters map[string]*cluster
	lock     sync.Mutex
}

// GetRegistry returns a global Registry.
func GetRegistry() *Registry {
	return &registry
}

// GetConn returns an etcd client connection associated with given endpoints.
func (r *Registry) GetConn(endpoints []string) (EtcdClient, error) {
	c, _ := r.getCluster(endpoints)
	// 得到的cli是原生的etcd client
	return c.getClient()
}

// Monitor monitors the key on given etcd endpoints, notify with the given UpdateListener.
func (r *Registry) Monitor(endpoints []string, key string, l UpdateListener) error {
	c, exists := r.getCluster(endpoints)
	// if exists, the existing values should be updated to the listener.
	if exists {
		// 内存中的kv传递给监听器.
		// 这里设计的目的是：防止etcd集群出现故障，这样内存中的数据依然可用，不会大规模的影响服务
		kvs := c.getCurrent(key)
		for _, kv := range kvs {
			l.OnAdd(kv)
		}
	}

	return c.monitor(key, l)
}

func (r *Registry) getCluster(endpoints []string) (c *cluster, exists bool) {
	clusterKey := getClusterKey(endpoints)
	r.lock.Lock()
	defer r.lock.Unlock()
	c, exists = r.clusters[clusterKey]
	if !exists {
		c = newCluster(endpoints)
		r.clusters[clusterKey] = c
	}

	return
}

type cluster struct {
	endpoints []string
	// endpoints 排序后，拼接成的字符串。目的是，标识一个etcd连接，用于单例初始化（在ResourceManager中）
	key string
	// {etcdhosts1: {key1: val1, key2: val2}, etcdhost2: {key11: val11, key22: val22}}
	values     map[string]map[string]string
	listeners  map[string][]UpdateListener
	watchGroup *threading.RoutineGroup
	done       chan lang.PlaceholderType
	lock       sync.Mutex
}

func newCluster(endpoints []string) *cluster {
	return &cluster{
		endpoints:  endpoints,
		key:        getClusterKey(endpoints),
		values:     make(map[string]map[string]string),
		listeners:  make(map[string][]UpdateListener),
		watchGroup: threading.NewRoutineGroup(),
		done:       make(chan lang.PlaceholderType),
	}
}

func (c *cluster) context(cli EtcdClient) context.Context {
	return contextx.ValueOnlyFrom(cli.Ctx())
}

// 获取一个单例EtcdClient，单例是通过connManager来管理的。
func (c *cluster) getClient() (EtcdClient, error) {
	// 通过资源管理器，来实现singleflight创建client
	val, err := connManager.GetResource(c.key, func() (io.Closer, error) {
		// 这里返回的是原生的Etcd Client
		return c.newClient()
	})
	if err != nil {
		return nil, err
	}

	return val.(EtcdClient), nil
}

func (c *cluster) getCurrent(key string) []KV {
	c.lock.Lock()
	defer c.lock.Unlock()

	var kvs []KV
	for k, v := range c.values[key] {
		kvs = append(kvs, KV{
			Key: k,
			Val: v,
		})
	}

	return kvs
}

// 依据参数kvs，和缓存进行对比，将新增和删除事件，通知到各个监听器
func (c *cluster) handleChanges(key string, kvs []KV) {
	var add []KV
	var remove []KV
	c.lock.Lock()
	listeners := append([]UpdateListener(nil), c.listeners[key]...)
	vals, ok := c.values[key]
	if !ok {
		// 没有缓存，新列表值，全部视为新增
		add = kvs
		vals = make(map[string]string)
		for _, kv := range kvs {
			vals[kv.Key] = kv.Val
		}
		// 更新缓存为新值列表
		c.values[key] = vals
	} else {
		// 有缓存
		m := make(map[string]string)
		for _, kv := range kvs {
			m[kv.Key] = kv.Val
		}
		// 缓存(vals)中有，新列表(m)中没有，则应该删除
		for k, v := range vals {
			if val, ok := m[k]; !ok || v != val {
				remove = append(remove, KV{
					Key: k,
					Val: v,
				})
			}
		}
		// 新列表(m)中有，缓存(vals)中没有，则应该添加
		for k, v := range m {
			if val, ok := vals[k]; !ok || v != val {
				add = append(add, KV{
					Key: k,
					Val: v,
				})
			}
		}
		// 更新缓存为新值列表
		c.values[key] = m
	}
	c.lock.Unlock()

	// 通知监听器添加
	for _, kv := range add {
		for _, l := range listeners {
			l.OnAdd(kv)
		}
	}
	// 通知监听器删除
	for _, kv := range remove {
		for _, l := range listeners {
			l.OnDelete(kv)
		}
	}
}

// 通过调用事件监听者，来处理事件
func (c *cluster) handleWatchEvents(key string, events []*clientv3.Event) {
	// 获取事件变更监听者
	c.lock.Lock()
	listeners := append([]UpdateListener(nil), c.listeners[key]...)
	c.lock.Unlock()

	for _, ev := range events {
		switch ev.Type {
		case clientv3.EventTypePut:
			c.lock.Lock()
			// 将变更，更新到缓存
			if vals, ok := c.values[key]; ok {
				vals[string(ev.Kv.Key)] = string(ev.Kv.Value)
			} else {
				c.values[key] = map[string]string{string(ev.Kv.Key): string(ev.Kv.Value)}
			}
			c.lock.Unlock()
			// PUT事件，依次调用监听者的OnAdd方法
			for _, l := range listeners {
				l.OnAdd(KV{
					Key: string(ev.Kv.Key),
					Val: string(ev.Kv.Value),
				})
			}
		case clientv3.EventTypeDelete:
			c.lock.Lock()
			// 将变更，更新到缓存
			if vals, ok := c.values[key]; ok {
				delete(vals, string(ev.Kv.Key))
			}
			c.lock.Unlock()
			// DELETE事件，依次调用监听者的OnDelete方法
			for _, l := range listeners {
				l.OnDelete(KV{
					Key: string(ev.Kv.Key),
					Val: string(ev.Kv.Value),
				})
			}
		default:
			logx.Errorf("Unknown event type: %v", ev.Type)
		}
	}
}

// 从etcd获取key前缀的值
func (c *cluster) load(cli EtcdClient, key string) int64 {
	var resp *clientv3.GetResponse
	for {
		var err error
		ctx, cancel := context.WithTimeout(c.context(cli), RequestTimeout)
		resp, err = cli.Get(ctx, makeKeyPrefix(key), clientv3.WithPrefix())
		cancel()
		if err == nil {
			break
		}

		logx.Error(err)
		time.Sleep(coolDownInterval)
	}

	var kvs []KV
	for _, ev := range resp.Kvs {
		kvs = append(kvs, KV{
			Key: string(ev.Key),
			Val: string(ev.Value),
		})
	}

	c.handleChanges(key, kvs)

	return resp.Header.Revision
}

func (c *cluster) monitor(key string, l UpdateListener) error {
	// 添加监听器
	c.lock.Lock()
	c.listeners[key] = append(c.listeners[key], l)
	c.lock.Unlock()

	// 得到的cli是原生的etcd client
	cli, err := c.getClient()
	if err != nil {
		return err
	}

	// 第一次初始化服务列表：从etcd server，获取到所有服务列表，并通知到各个事件处理器
	rev := c.load(cli, key)
	// 启动协程监听key前缀变更事件，接收到事件后，调用各个事件处理器
	c.watchGroup.Run(func() {
		c.watch(cli, key, rev)
	})

	return nil
}

// 创建etcd client
func (c *cluster) newClient() (EtcdClient, error) {
	cli, err := NewClient(c.endpoints)
	if err != nil {
		return nil, err
	}

	go c.watchConnState(cli)

	return cli, nil
}

func (c *cluster) reload(cli EtcdClient) {
	c.lock.Lock()
	close(c.done)
	c.watchGroup.Wait()
	c.done = make(chan lang.PlaceholderType)
	c.watchGroup = threading.NewRoutineGroup()
	var keys []string
	for k := range c.listeners {
		keys = append(keys, k)
	}
	c.lock.Unlock()

	for _, key := range keys {
		k := key
		c.watchGroup.Run(func() {
			rev := c.load(cli, k)
			c.watch(cli, k, rev)
		})
	}
}

func (c *cluster) watch(cli EtcdClient, key string, rev int64) {
	// 用一个死循环，来处理watch错误情况
	for {
		if c.watchStream(cli, key, rev) {
			return
		}
	}
}

// 监听etcd key前缀变化事件
func (c *cluster) watchStream(cli EtcdClient, key string, rev int64) bool {
	var rch clientv3.WatchChan
	if rev != 0 {
		rch = cli.Watch(clientv3.WithRequireLeader(c.context(cli)), makeKeyPrefix(key), clientv3.WithPrefix(), clientv3.WithRev(rev+1))
	} else {
		rch = cli.Watch(clientv3.WithRequireLeader(c.context(cli)), makeKeyPrefix(key), clientv3.WithPrefix())
	}

	for {
		select {
		case wresp, ok := <-rch:
			if !ok {
				logx.Error("etcd monitor chan has been closed")
				return false
			}
			if wresp.Canceled {
				logx.Errorf("etcd monitor chan has been canceled, error: %v", wresp.Err())
				return false
			}
			if wresp.Err() != nil {
				logx.Error(fmt.Sprintf("etcd monitor chan error: %v", wresp.Err()))
				return false
			}

			// 变更事件处理
			c.handleWatchEvents(key, wresp.Events)
		case <-c.done:
			return true
		}
	}
}

func (c *cluster) watchConnState(cli EtcdClient) {
	watcher := newStateWatcher()
	watcher.addListener(func() {
		go c.reload(cli)
	})
	watcher.watch(cli.ActiveConnection())
}

// DialClient dials an etcd cluster with given endpoints.
func DialClient(endpoints []string) (EtcdClient, error) {
	cfg := clientv3.Config{
		Endpoints:            endpoints,
		AutoSyncInterval:     autoSyncInterval,
		DialTimeout:          DialTimeout,
		DialKeepAliveTime:    dialKeepAliveTime,
		DialKeepAliveTimeout: DialTimeout,
		RejectOldCluster:     true,
	}
	if account, ok := GetAccount(endpoints); ok {
		cfg.Username = account.User
		cfg.Password = account.Pass
	}
	if tlsCfg, ok := GetTLS(endpoints); ok {
		cfg.TLS = tlsCfg
	}

	return clientv3.New(cfg)
}

func getClusterKey(endpoints []string) string {
	sort.Strings(endpoints)
	return strings.Join(endpoints, endpointsSeparator)
}

func makeKeyPrefix(key string) string {
	return fmt.Sprintf("%s%c", key, Delimiter)
}
