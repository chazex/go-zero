package load

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/zeromicro/go-zero/core/collection"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/syncx"
	"github.com/zeromicro/go-zero/core/timex"
)

const (
	defaultBuckets = 50
	defaultWindow  = time.Second * 5
	// using 1000m notation, 900m is like 90%, keep it as var for unit test
	defaultCpuThreshold = 900
	defaultMinRt        = float64(time.Second / time.Millisecond)
	// moving average hyperparameter beta for calculating requests on the fly
	flyingBeta      = 0.9
	coolOffDuration = time.Second
)

var (
	// ErrServiceOverloaded is returned by Shedder.Allow when the service is overloaded.
	ErrServiceOverloaded = errors.New("service overloaded")

	// default to be enabled
	enabled = syncx.ForAtomicBool(true)
	// default to be enabled
	logEnabled = syncx.ForAtomicBool(true)
	// make it a variable for unit test
	systemOverloadChecker = func(cpuThreshold int64) bool {
		return stat.CpuUsage() >= cpuThreshold
	}
)

type (
	// Promise 回调函数
	// A Promise interface is returned by Shedder.Allow to let callers tell
	// whether the processing request is successful or not.
	Promise interface {
		// Pass 请求成功时回调此函数
		// Pass lets the caller tell that the call is successful.
		Pass()
		// Fail 请求失败时回调此函数
		// Fail lets the caller tell that the call is failed.
		Fail()
	}

	// Shedder 降载接口定义
	// Shedder is the interface that wraps the Allow method.
	Shedder interface {
		// Allow 降载检查
		// 1. 允许调用，需手动执行 Promise.accept()/reject()上报实际执行任务结果
		// 2. 拒绝调用，将会直接返回err：服务过载错误 ErrServiceOverloaded
		// Allow returns the Promise if allowed, otherwise ErrServiceOverloaded.
		Allow() (Promise, error)
	}

	// ShedderOption lets caller customize the Shedder.
	ShedderOption func(opts *shedderOptions)

	shedderOptions struct {
		window       time.Duration
		buckets      int
		cpuThreshold int64
	}

	adaptiveShedder struct {
		cpuThreshold int64
		windows      int64
		// 正在处理中的请求数量
		flying int64
		// 处理中请求的滑动平均值（滞后于flying）
		avgFlying     float64
		avgFlyingLock syncx.SpinLock
		// 记录的是cpu最近一次判断过载的时间
		overloadTime *syncx.AtomicDuration
		// 用于标识上一次的请求是否被drop(用于快速判断是否为冷却期： 如果上一次的请求没有drop， 则标识已经过了冷却期。 如果上一次请求被drop则还要做具体判断)
		droppedRecently *syncx.AtomicBool
		// 请求通过 （滑动窗口，可以防止毛刺）
		passCounter *collection.RollingWindow
		// 请求消耗的时间 （滑动窗口，可以防止毛刺）
		rtCounter *collection.RollingWindow
	}
)

// Disable lets callers disable load shedding.
func Disable() {
	enabled.Set(false)
}

// DisableLog disables the stat logs for load shedding.
func DisableLog() {
	logEnabled.Set(false)
}

// NewAdaptiveShedder returns an adaptive shedder.
// opts can be used to customize the Shedder.
func NewAdaptiveShedder(opts ...ShedderOption) Shedder {
	if !enabled.True() {
		return newNopShedder()
	}

	options := shedderOptions{
		window:       defaultWindow,
		buckets:      defaultBuckets,
		cpuThreshold: defaultCpuThreshold,
	}
	for _, opt := range opts {
		opt(&options)
	}
	bucketDuration := options.window / time.Duration(options.buckets)
	return &adaptiveShedder{
		cpuThreshold:    options.cpuThreshold,
		windows:         int64(time.Second / bucketDuration),
		overloadTime:    syncx.NewAtomicDuration(),
		droppedRecently: syncx.NewAtomicBool(),
		passCounter: collection.NewRollingWindow(options.buckets, bucketDuration,
			collection.IgnoreCurrentBucket()),
		rtCounter: collection.NewRollingWindow(options.buckets, bucketDuration,
			collection.IgnoreCurrentBucket()),
	}
}

// Allow implements Shedder.Allow.
func (as *adaptiveShedder) Allow() (Promise, error) {
	if as.shouldDrop() {
		// 请求应该被丢弃
		as.droppedRecently.Set(true)

		return nil, ErrServiceOverloaded
	}

	// 正在处理的请求数量+1
	as.addFlying(1)

	return &promise{
		start:   timex.Now(),
		shedder: as,
	}, nil
}

func (as *adaptiveShedder) addFlying(delta int64) {
	// 增加delta，并返回修改后的值， 即flying是目前正在处理中的请求值
	flying := atomic.AddInt64(&as.flying, delta)
	// update avgFlying when the request is finished.
	// this strategy makes avgFlying have a little bit lag against flying, and smoother.
	// when the flying requests increase rapidly, avgFlying increase slower, accept more requests.
	// when the flying requests drop rapidly, avgFlying drop slower, accept less requests.
	// it makes the service to serve as more requests as possible.
	// 请求结束时， 更新aveFlying
	// avgFlying策略，相对于flying具有一点点滞后性，avgFlying更平滑
	// 当 flying的数量快速增加， avgFlying缓慢增加时，可以接受更多的请求
	// 当 flying的数量快速下降时，avgFlying也缓慢下降时，可以接受较少的请求
	// 目的是为了让服务尽可能的处理更多的请求。

	// 这块的原理大概是这样：　当我们的服务负载一直比较平稳时，突然来了一个高峰, 请求量激增，但是这个高峰很短暂，它在flying上面体现很明显，但此时在avgFlying还并没有体现出来，或者后续也不一定体现的出来。
	// 这个短暂的高峰，我们认为服务可以吃下这波流量。 所以此时并不急于拒绝请求。 因为这波短暂的高峰过后，又恢复如常。
	// 应对下降也是同样的道理，如果服务一直处于高负载，突然来了一个低谷（请求量骤降),但是这个低谷很短暂， 它在flying上面体现很明显，但此时在avgFlying还并没有体现出来，或者后续也不一定体现的出来。
	// 这个短暂的低谷，我们不能认为服务的压力就降低了（后面还有大量的请求）。 所以此时应该接受较少的请求。 因为这波短暂的低谷过后，还有可能是大量的高峰期。

	// 所以后面在判断是否为高吞吐时，只有avgFlying和flying都很高时，才会认为是高吞吐
	// 这里有个疑问，对于avgFlying很高，但是flying很少时，不也应该是高吞吐吗，此时应该拒掉请求吧？
	if delta < 0 {
		as.avgFlyingLock.Lock()
		// 平均值占比90%， 其他占10%
		as.avgFlying = as.avgFlying*flyingBeta + float64(flying)*(1-flyingBeta)
		as.avgFlyingLock.Unlock()
	}
}

// 如果平均正在请求数 > 最大允许正在请求数 && 当前正在处理的请求数 > 最大允许正在请求数, 则认为服务压力比较大，返回true
func (as *adaptiveShedder) highThru() bool {
	as.avgFlyingLock.Lock()
	avgFlying := as.avgFlying
	as.avgFlyingLock.Unlock()
	maxFlight := as.maxFlight()
	return int64(avgFlying) > maxFlight && atomic.LoadInt64(&as.flying) > maxFlight
}

func (as *adaptiveShedder) maxFlight() int64 {
	// windows = buckets per second
	// maxQPS = maxPASS * windows
	// minRT = min average response time in milliseconds
	// maxQPS * minRT / milliseconds_per_second
	return int64(math.Max(1, float64(as.maxPass()*as.windows)*(as.minRt()/1e3)))
}

func (as *adaptiveShedder) maxPass() int64 {
	var result float64 = 1

	as.passCounter.Reduce(func(b *collection.Bucket) {
		if b.Sum > result {
			result = b.Sum
		}
	})

	return int64(result)
}

func (as *adaptiveShedder) minRt() float64 {
	result := defaultMinRt

	as.rtCounter.Reduce(func(b *collection.Bucket) {
		if b.Count <= 0 {
			return
		}

		avg := math.Round(b.Sum / float64(b.Count))
		if avg < result {
			result = avg
		}
	})

	return result
}

func (as *adaptiveShedder) shouldDrop() bool {
	// （cpu达到阈值，或者 在冷却期间） && 高吞吐  => 过载保护
	if as.systemOverloaded() || as.stillHot() {
		if as.highThru() {
			flying := atomic.LoadInt64(&as.flying)
			as.avgFlyingLock.Lock()
			avgFlying := as.avgFlying
			as.avgFlyingLock.Unlock()
			msg := fmt.Sprintf(
				"dropreq, cpu: %d, maxPass: %d, minRt: %.2f, hot: %t, flying: %d, avgFlying: %.2f",
				stat.CpuUsage(), as.maxPass(), as.minRt(), as.stillHot(), flying, avgFlying)
			logx.Error(msg)
			stat.Report(msg)
			return true
		}
	}

	return false
}

func (as *adaptiveShedder) stillHot() bool {
	if !as.droppedRecently.True() {
		return false
	}

	overloadTime := as.overloadTime.Load()
	if overloadTime == 0 {
		return false
	}

	// 距离上次cpu被判断为过载的时间，小于1s， 我们仍然认为是过载
	if timex.Since(overloadTime) < coolOffDuration {
		return true
	}

	as.droppedRecently.Set(false)
	return false
}

// 判断服务是否过载：　判断cpu使用率是否高于我们设置的值（as.cpuThreshold）
func (as *adaptiveShedder) systemOverloaded() bool {
	if !systemOverloadChecker(as.cpuThreshold) {
		return false
	}

	// 留存一下cpu过载的时间
	as.overloadTime.Set(timex.Now())
	return true
}

// WithBuckets customizes the Shedder with given number of buckets.
func WithBuckets(buckets int) ShedderOption {
	return func(opts *shedderOptions) {
		opts.buckets = buckets
	}
}

// WithCpuThreshold customizes the Shedder with given cpu threshold.
func WithCpuThreshold(threshold int64) ShedderOption {
	return func(opts *shedderOptions) {
		opts.cpuThreshold = threshold
	}
}

// WithWindow customizes the Shedder with given
func WithWindow(window time.Duration) ShedderOption {
	return func(opts *shedderOptions) {
		opts.window = window
	}
}

type promise struct {
	start   time.Duration
	shedder *adaptiveShedder
}

func (p *promise) Fail() {
	p.shedder.addFlying(-1)
}

func (p *promise) Pass() {
	// 计算本次请求 消耗时间， 单位毫秒
	rt := float64(timex.Since(p.start)) / float64(time.Millisecond)
	p.shedder.addFlying(-1)
	p.shedder.rtCounter.Add(math.Ceil(rt))
	p.shedder.passCounter.Add(1)
}
