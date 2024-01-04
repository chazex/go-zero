package discov

import (
	"time"

	"github.com/zeromicro/go-zero/core/discov/internal"
	"github.com/zeromicro/go-zero/core/lang"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/core/syncx"
	"github.com/zeromicro/go-zero/core/threading"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type (
	// PubOption defines the method to customize a Publisher.
	PubOption func(client *Publisher)

	// A Publisher can be used to publish the value to an etcd cluster on the given key.
	Publisher struct {
		// etcd 地址
		endpoints []string
		// etcd key
		key string
		// 存储到etcd中的真实key
		fullKey string
		id      int64
		// 服务监听地址
		value      string
		lease      clientv3.LeaseID
		quit       *syncx.DoneChan
		pauseChan  chan lang.PlaceholderType
		resumeChan chan lang.PlaceholderType
	}
)

// NewPublisher returns a Publisher.
// endpoints is the hosts of the etcd cluster.
// key:value are a pair to be published.
// opts are used to customize the Publisher.
func NewPublisher(endpoints []string, key, value string, opts ...PubOption) *Publisher {
	publisher := &Publisher{
		endpoints:  endpoints,
		key:        key,
		value:      value,
		quit:       syncx.NewDoneChan(),
		pauseChan:  make(chan lang.PlaceholderType),
		resumeChan: make(chan lang.PlaceholderType),
	}

	for _, opt := range opts {
		opt(publisher)
	}

	return publisher
}

// KeepAlive keeps key:value alive.
func (p *Publisher) KeepAlive() error {
	cli, err := p.doRegister()
	if err != nil {
		return err
	}

	// 注册监听函数，当服务关闭时，需要关闭服务注册
	proc.AddWrapUpListener(func() {
		p.Stop()
	})

	// 异步持续续租lease
	return p.keepAliveAsync(cli)
}

// Pause pauses the renewing of key:value.
func (p *Publisher) Pause() {
	// 暂停续约
	p.pauseChan <- lang.Placeholder
}

// Resume resumes the renewing of key:value.
func (p *Publisher) Resume() {
	// 恢复续约
	p.resumeChan <- lang.Placeholder
}

// Stop stops the renewing and revokes the registration.
func (p *Publisher) Stop() {
	p.quit.Close()
}

func (p *Publisher) doKeepAlive() error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		select {
		case <-p.quit.Done():
			return nil
		default:
			cli, err := p.doRegister()
			if err != nil {
				logx.Errorf("etcd publisher doRegister: %s", err.Error())
				break
			}

			if err := p.keepAliveAsync(cli); err != nil {
				logx.Errorf("etcd publisher keepAliveAsync: %s", err.Error())
				break
			}

			return nil
		}
	}

	return nil
}

func (p *Publisher) doRegister() (internal.EtcdClient, error) {
	// 获取etcd实例：得到的cli是原生的etcd client
	cli, err := internal.GetRegistry().GetConn(p.endpoints)
	if err != nil {
		return nil, err
	}

	// 存储到etcd
	p.lease, err = p.register(cli)
	return cli, err
}

func (p *Publisher) keepAliveAsync(cli internal.EtcdClient) error {
	// cli是原生的etcd client.
	// 保持租约
	ch, err := cli.KeepAlive(cli.Ctx(), p.lease)
	if err != nil {
		return err
	}

	threading.GoSafe(func() {
		for {
			select {
			case _, ok := <-ch:
				if !ok {
					// channel关闭，此时一般是出错了。废弃lease，并重新初始化
					p.revoke(cli)
					// 重新初始化
					if err := p.doKeepAlive(); err != nil {
						logx.Errorf("etcd publisher KeepAlive: %s", err.Error())
					}
					return
				}
			case <-p.pauseChan:
				logx.Infof("paused etcd renew, key: %s, value: %s", p.key, p.value)
				// 废弃lease
				p.revoke(cli)
				select {
				case <-p.resumeChan:
					// 接收到恢复指令：目前没找到那里会下发这个指令
					if err := p.doKeepAlive(); err != nil {
						logx.Errorf("etcd publisher KeepAlive: %s", err.Error())
					}
					return
				case <-p.quit.Done():
					// 这里是程序正常退出，服务下线.
					return
				}
			case <-p.quit.Done():
				// 这里是程序正常退出，服务下线，删除租约.
				// 服务收到退出信号，开始依次执行每一个wrapUpListeners，此时会执行p.Stop()，就会关闭这个channel
				p.revoke(cli)
				return
			}
		}
	})

	return nil
}

// 将服务地址存储到etcd
func (p *Publisher) register(client internal.EtcdClient) (clientv3.LeaseID, error) {
	// 创建一个新的租约
	resp, err := client.Grant(client.Ctx(), TimeToLive)
	if err != nil {
		return clientv3.NoLease, err
	}

	lease := resp.ID
	if p.id > 0 {
		p.fullKey = makeEtcdKey(p.key, p.id)
	} else {
		p.fullKey = makeEtcdKey(p.key, int64(lease))
	}
	// key，val（服务监听地址），租约put到etcd server
	_, err = client.Put(client.Ctx(), p.fullKey, p.value, clientv3.WithLease(lease))

	return lease, err
}

// 废弃lease
func (p *Publisher) revoke(cli internal.EtcdClient) {
	if _, err := cli.Revoke(cli.Ctx(), p.lease); err != nil {
		logx.Errorf("etcd publisher revoke: %s", err.Error())
	}
}

// WithId customizes a Publisher with the id.
func WithId(id int64) PubOption {
	return func(publisher *Publisher) {
		publisher.id = id
	}
}

// WithPubEtcdAccount provides the etcd username/password.
func WithPubEtcdAccount(user, pass string) PubOption {
	return func(pub *Publisher) {
		RegisterAccount(pub.endpoints, user, pass)
	}
}

// WithPubEtcdTLS provides the etcd CertFile/CertKeyFile/CACertFile.
func WithPubEtcdTLS(certFile, certKeyFile, caFile string, insecureSkipVerify bool) PubOption {
	return func(pub *Publisher) {
		logx.Must(RegisterTLS(pub.endpoints, certFile, certKeyFile, caFile, insecureSkipVerify))
	}
}
