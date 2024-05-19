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

// https://go-zero.dev/docs/tutorials/service/governance/lb
// P2C算法是一种改进的随机算法，它可以避免最劣选择和负载不均衡的情况。
//P2C算法的核心思想是：从所有可用节点中随机选择两个节点，然后根据这两个节点的负载情况选择一个负载较小的节点。
//这样做的好处在于，如果只随机选择一个节点，可能会选择到负载较高的节点，从而导致负载不均衡；而选择两个节点，则可以进行比较，从而避免最劣选择。

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

	// 保存grpc所有可用连接
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

func (p *p2cPicker) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
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
			// 这块没看懂: 现在懂了，b选择在[0,n-1)范围内选取随机值，是因为后面要做b++，为了不超过限制，所以必须是在[0,n-1)内选取
			// 这里我们再看，为什么要在b >= a时, b++, 首先比较好理解的时，当b==a时，表示两次选取的节点一样，这不符合逻辑，所以让b后移一位，让他们有差异。
			// 当b > a 时，为什么要+1呢，因为b是从[0,n-1)内选取的，所以它永远选不到n，所以我们后移一位，让它可以选择到n。
			// [0, n)
			a := p.r.Intn(len(p.conns))
			// [0, n-1)
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
	// 总请求数量+1
	atomic.AddInt64(&chosen.requests, 1)

	return balancer.PickResult{
		SubConn: chosen.conn,
		Done:    p.buildDoneFunc(chosen),
	}, nil
}

// 在原生grpc中，rpc调用完成之后，会调用这个函数返回的函数。

func (p *p2cPicker) buildDoneFunc(c *subConn) func(info balancer.DoneInfo) {
	start := int64(timex.Now())
	return func(info balancer.DoneInfo) {
		// 请求完成，对正在做的请求-1
		atomic.AddInt64(&c.inflight, -1)
		now := timex.Now()
		// 保存最新一次请求结束时的时间，并取出上次请求结束时的时间点
		last := atomic.SwapInt64(&c.last, int64(now))

		// td  最近两次请求结束时间的差值
		td := int64(now) - last
		if td < 0 {
			td = 0
		}
		// // 用牛顿冷却定律中的衰减函数模型计算EWMA算法中的β值，这里叫w
		// math.Exp(x float)函数返回e**x, 即返回e的x次方的值。
		// 由于计算的是-td, 所以计算的是 (1/e) 的 td/decayTime次方的值。 (1/e)**n： 1/e的n次方，n为最近的两个RPC完成时间包含了几个衰变期
		// 如果服务不繁忙，那么两次请求的时间就会很长，td变大，那么n就会变大，n越大，w值越小。 w表示加权下降的速率，w值越小，加权下降的越快(代表上一次的值的加权越小)
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

	// 比较两个节点的负载(最终赋值： c1 load 低)
	if c1.load() > c2.load() {
		c1, c2 = c2, c1
	}

	// 现在c1变量存储的是的load低的连接，c2变量存储的是的load高的连接。
	// 但是如果c2连接，本次pick和上次pick之间超过了1秒钟,则强制使用c2连接 （为什么？，因为长时间没有使用，所以会导致load一直没法更新？）
	pick := atomic.LoadInt64(&c2.pick)
	// 原子操作： c2.pick == pick时，将c2.pick 设置为 start （CompareAndSwapInt64函数的功能为，比较c2.pick和pick，如果是，将c2.pick更新为start）
	if start-pick > forcePick && atomic.CompareAndSwapInt64(&c2.pick, pick, start) {
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
	// 正在处理的请求数, 一个subConn可以同时处理多个请求
	inflight int64
	// ewma success, 用来判断此服务节点是否健康
	success uint64
	// 此conn，一共处理的请求次数
	requests int64
	// 最近一次请求处理完成时间
	last int64
	// 最近一次做负载均衡时，被pick的时间
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
