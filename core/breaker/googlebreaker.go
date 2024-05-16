package breaker

import (
	"math"
	"time"

	"github.com/zeromicro/go-zero/core/collection"
	"github.com/zeromicro/go-zero/core/mathx"
)

const (
	// 250ms for bucket duration
	window  = time.Second * 10
	buckets = 40
	// 在时间窗口内统计， 当成功的请求超过总量请求的2/3，不容熔断。 2/3的是K的倒数
	k          = 1.5
	protection = 5
)

// 利用滑动窗口（10s）存储过去10s内的请求总数，和成功总数。 然后计算当前请求是需要拒绝还是接收。

// googleBreaker is a netflixBreaker pattern from google.
// see Client-Side Throttling section in https://landing.google.com/sre/sre-book/chapters/handling-overload/
type googleBreaker struct {
	k float64
	// 内部有一个滑动窗口，记录了过去一段时间（10s）的请求总量，和拒绝状况。
	stat  *collection.RollingWindow
	proba *mathx.Proba
}

func newGoogleBreaker() *googleBreaker {
	bucketDuration := time.Duration(int64(window) / int64(buckets))
	st := collection.NewRollingWindow(buckets, bucketDuration)
	return &googleBreaker{
		stat:  st,
		k:     k,
		proba: mathx.NewProba(),
	}
}

func (b *googleBreaker) accept() error {
	// accepts 是整个滑动窗口内的 成功数， total 是总数
	accepts, total := b.history()
	weightedAccepts := b.k * float64(accepts)
	// https://landing.google.com/sre/sre-book/chapters/handling-overload/#eq2101
	// total+1的作用是为了，保证分母>0, 当过去一段时间没有请求到来，则total可能为0
	//protection是一个保护阈值，设置为5。这是为了保证当请求量较少的时候不会轻易触发熔断。
	// 如果我们假设保护阈值protection为5，总请求数total为10，成功请求数accepts为7（也就是失败请求数为3）。
	//
	//当保护阈值大于总请求次数，即protection > total：此时无论是成功率是多少，都通过
	//比如，当total为2（小于protection的5），即使有1次请求失败，计算出的dropRatio依然为0（(2 - 5 - 1.5*1) /(2+1)）。也就是说，即使50%的请求失败，由于总的请求次数还没有达到保护阈值，系统认为样本太小，不会触发熔断。
	//
	//当保护阈值小于或等于总请求次数，即protection <= total：
	//比如，当total等于10，protection等于5，成功的接受次数为7，失败3次。此时，计算的dropRatio等于(10 - 5 - 1.5*7)/(10+1)，。因为dropRatio小于等于0，请求会被允许通过。
	//
	//再比如，当total等于10，protection等于5，成功的接受次数为4，失败6次。此时，计算的dropRatio等于(10 - 5 - 1.5*4)/(10+1)，。因为dropRatio大于0，请求不会被允许通过，会触发熔断操作。
	//
	//再比如，当total等于10，protection等于5，成功的接受次数为3，失败7次。此时，计算的dropRatio等于(10 - 5 - 1.5*2)/(10+1)，。因为dropRatio大于0，请求不会被允许通过，会触发熔断操作。
	//
	//所以，保护阈值的设置可以防止当样本较小的时候过早触发熔断，也保证了当有足够的样本（即请求次数大于阈值）且失败率过高时，能够及时触发熔断。
	dropRatio := math.Max(0, (float64(total-protection)-weightedAccepts)/float64(total+1))
	if dropRatio <= 0 {
		// 通过
		return nil
	}

	// 走到这里，代表dropRatio>0, 此时说明我们的请求成功率已经低于我们前面设置的值，需要走熔断逻辑
	// 但是这并不是直接就把所有的请求都做熔断，先生成的随机数[0.0,1.0)， 让随机数和我们的dropRatio对比， 随机数小于dropRatio时，做熔断，否则仍然放行。
	// 原因是，当dropRatio很小时，说明系统现在的情况是， 成功率只是稍微低于我们前面设置的值，此时 随机数小于dropRatio的情况概率也比较低。 所以熔断的数量并不多。
	// dropRatio变得比较大时，说明系统现在的情况是， 成功率已经很低了， 此时 随机数小于dropRatio的情况概率也变得高了很多。 所以熔断的数量遍多了。
	// 这么做的主要原因是，让系统比较平滑，不是达到某个阈值，直接拒绝所有请求。

	// 因为weightedAccepts>=0， 所以dropRatio 肯定是<1的。
	// weightedAccepts越大，代表成功的数量越多, 此时dropRatio越小，反之亦然。
	// dropRatio越小，则生成的随机数[0.0,1.0) < dropRatio的概率越低。
	// 这么做的原因猜测： 因为当dropRatio > 0时，（如果按照total=100来算的话成功数量占 63.3（95/1.5）， 如果按照1000来算的话，成功的数量在663(995/1.5)），此时失败的数量已经达到一定的级别了。
	// 就可以认为需要按照一定的几率来减轻服务器的压力。
	if b.proba.TrueOnProba(dropRatio) {
		// 生成的[0.0,1.0)的随机数 < dropRatio, 则返回错误
		return ErrServiceUnavailable
	}

	return nil
}

func (b *googleBreaker) allow() (internalPromise, error) {
	if err := b.accept(); err != nil {
		// 被拒绝
		return nil, err
	}

	return googlePromise{
		b: b,
	}, nil
}

func (b *googleBreaker) doReq(req func() error, fallback func(err error) error, acceptable Acceptable) error {
	if err := b.accept(); err != nil {
		// 被拒绝
		if fallback != nil {
			return fallback(err)
		}

		return err
	}

	defer func() {
		// 发生panic, 失败数量+1
		if e := recover(); e != nil {
			b.markFailure()
			panic(e)
		}
	}()

	// 执行实际请求函数
	err := req()
	if acceptable(err) {
		// 实际执行：b.stat.Add(1)
		// 也就是说：内部指标统计成功+1
		b.markSuccess()
	} else {
		// 原理同上
		b.markFailure()
	}

	return err
}

func (b *googleBreaker) markSuccess() {
	b.stat.Add(1)
}

func (b *googleBreaker) markFailure() {
	b.stat.Add(0)
}

func (b *googleBreaker) history() (accepts, total int64) {
	b.stat.Reduce(func(b *collection.Bucket) {
		accepts += int64(b.Sum)
		total += b.Count
	})

	return
}

type googlePromise struct {
	b *googleBreaker
}

func (p googlePromise) Accept() {
	p.b.markSuccess()
}

func (p googlePromise) Reject() {
	p.b.markFailure()
}
