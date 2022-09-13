package p2c

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/syncx"
	"github.com/zeromicro/go-zero/core/timex"
	"github.com/zeromicro/go-zero/zrpc/internal/codes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
)

const (
	// Name is the name of p2c balancer.
	Name = "p2c_ewma"

	//f 衰变时间， 衰变周期
	decayTime       = int64(time.Second * 10) // default value from finagle
	forcePick       = int64(time.Second)
	initSuccess     = 1000
	throttleSuccess = initSuccess / 2
	penalty         = int64(math.MaxInt32)
	pickTimes       = 3
	logInterval     = time.Minute
)

var emptyPickResult balancer.PickResult

func init() {
	balancer.Register(newBuilder())
}

type p2cPickerBuilder struct{}

func (b *p2cPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	readySCs := info.ReadySCs
	if len(readySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	var conns []*subConn
	for conn, connInfo := range readySCs {
		conns = append(conns, &subConn{
			addr:    connInfo.Address,
			conn:    conn,
			success: initSuccess,
		})
	}

	return &p2cPicker{
		conns: conns,
		r:     rand.New(rand.NewSource(time.Now().UnixNano())),
		stamp: syncx.NewAtomicDuration(),
	}
}

func newBuilder() balancer.Builder {
	// 使用grpc原生的baseBuilder封装的
	return base.NewBalancerBuilder(Name, new(p2cPickerBuilder), base.Config{HealthCheck: true})
}

type p2cPicker struct {
	conns []*subConn
	r     *rand.Rand
	stamp *syncx.AtomicDuration
	lock  sync.Mutex
}

func (p *p2cPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	var chosen *subConn
	switch len(p.conns) {
	case 0:
		// 没有节点
		return emptyPickResult, balancer.ErrNoSubConnAvailable
	case 1:
		// 只有一个节点，直接返回供gRPC使用
		chosen = p.choose(p.conns[0], nil)
	case 2:
		// 有两个服务节点，通过 EWMA值 计算负载，并返回负载低的节点返回供 gRPC 使用
		chosen = p.choose(p.conns[0], p.conns[1])
	default:
		// 有多个服务节点，此时通过 p2c 算法选出两个节点，比较负载情况，返回负载低的节点供 gRPC 使用
		var node1, node2 *subConn
		// 3次随机选择两个节点
		for i := 0; i < pickTimes; i++ {
			a := p.r.Intn(len(p.conns))
			b := p.r.Intn(len(p.conns) - 1)
			if b >= a {
				b++
			}
			node1 = p.conns[a]
			node2 = p.conns[b]
			if node1.healthy() && node2.healthy() {
				// 选出的两个节点都健康，则停止
				break
			}
		}

		// 通过 EWMA值 计算负载，并返回负载低的节点返回供 gRPC 使用
		chosen = p.choose(node1, node2)
	}

	// 正在做的请求 + 1
	atomic.AddInt64(&chosen.inflight, 1)
	atomic.AddInt64(&chosen.requests, 1)

	return balancer.PickResult{
		SubConn: chosen.conn,
		Done:    p.buildDoneFunc(chosen),
	}, nil
}

func (p *p2cPicker) buildDoneFunc(c *subConn) func(info balancer.DoneInfo) {
	start := int64(timex.Now())
	return func(info balancer.DoneInfo) {
		// 请求完成，对正在做的请求-1
		atomic.AddInt64(&c.inflight, -1)
		now := timex.Now()
		// 保存本次请求结束时的时间点，并取出上次请求时的时间点
		last := atomic.SwapInt64(&c.last, int64(now))

		td := int64(now) - last
		if td < 0 {
			td = 0
		}
		// // 用牛顿冷却定律中的衰减函数模型计算EWMA算法中的β值，这里叫w
		// (1/e)**n： 1/e的n次方，n为两个RPC完成时间包含了几个衰变期
		// 如果服务不繁忙，那么两次请求的时间就会很长，那么n就会变大，n越大，w值越小。 w表示加权下降的速率，w值越小，加权下降的越快(代表上一次的值的加权越小)
		w := math.Exp(float64(-td) / float64(decayTime))
		// 计算本次请求延迟，并保存 （从pick 到 RPC请求完成）
		lag := int64(now) - start
		if lag < 0 {
			lag = 0
		}
		olag := atomic.LoadUint64(&c.lag)
		if olag == 0 {
			w = 0
		}
		// 请求延迟维度的ewma计算
		// 计算这一次的ewma值
		// lag 是本次的实际值（本次RPC请求时间）
		atomic.StoreUint64(&c.lag, uint64(float64(olag)*w+float64(lag)*(1-w)))
		success := initSuccess
		if info.Err != nil && !codes.Acceptable(info.Err) {
			success = 0
		}
		// osucc 上一次的ewma值
		osucc := atomic.LoadUint64(&c.success)
		// 请求是否成功维度的ewma计算
		// 计算这一次的ewma值
		// success 是本次的实际值（成功为1000，失败为0）
		atomic.StoreUint64(&c.success, uint64(float64(osucc)*w+float64(success)*(1-w)))

		stamp := p.stamp.Load()
		if now-stamp >= logInterval {
			if p.stamp.CompareAndSwap(stamp, now) {
				p.logStats()
			}
		}
	}
}

func (p *p2cPicker) choose(c1, c2 *subConn) *subConn {
	start := int64(timex.Now())
	if c2 == nil {
		atomic.StoreInt64(&c1.pick, start)
		return c1
	}

	// 比较两个节点的负载
	if c1.load() > c2.load() {
		c1, c2 = c2, c1
	}

	pick := atomic.LoadInt64(&c2.pick)
	if start-pick > forcePick && atomic.CompareAndSwapInt64(&c2.pick, pick, start) {
		// 强制pick?
		return c2
	}

	atomic.StoreInt64(&c1.pick, start)
	return c1
}

func (p *p2cPicker) logStats() {
	var stats []string

	p.lock.Lock()
	defer p.lock.Unlock()

	for _, conn := range p.conns {
		stats = append(stats, fmt.Sprintf("conn: %s, load: %d, reqs: %d",
			conn.addr.Addr, conn.load(), atomic.SwapInt64(&conn.requests, 0)))
	}

	logx.Statf("p2c - %s", strings.Join(stats, "; "))
}

type subConn struct {
	// ewma lag， 相当于平均耗时
	lag uint64
	// 正在处理的请求数
	inflight int64
	// ewma success, 用来判断此服务节点是否健康
	success  uint64
	requests int64
	// 最后一次请求处理完成时间
	last int64
	pick int64
	addr resolver.Address
	conn balancer.SubConn
}

func (c *subConn) healthy() bool {
	// 是否成功的ewma > initSuccess/2
	// 这是为啥
	return atomic.LoadUint64(&c.success) > throttleSuccess
}

// 计算一个服务节点的load
func (c *subConn) load() int64 {
	// 通过 EWMA 计算节点的负载情况； 加 1 是为了避免为 0 的情况
	// plus one to avoid multiply zero
	lag := int64(math.Sqrt(float64(atomic.LoadUint64(&c.lag) + 1)))
	// ewma 相当于平均请求耗时，inflight 是当前节点正在处理请求的数量，相乘大致计算出了当前节点的网络负载。
	load := lag * (atomic.LoadInt64(&c.inflight) + 1)
	if load == 0 {
		return penalty
	}

	return load
}
