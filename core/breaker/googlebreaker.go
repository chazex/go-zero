package breaker

import (
	"math"
	"time"

	"github.com/zeromicro/go-zero/core/collection"
	"github.com/zeromicro/go-zero/core/mathx"
)

const (
	// 250ms for bucket duration
	window     = time.Second * 10
	buckets    = 40
	k          = 1.5
	protection = 5
)

// googleBreaker is a netflixBreaker pattern from google.
// see Client-Side Throttling section in https://landing.google.com/sre/sre-book/chapters/handling-overload/
type googleBreaker struct {
	k     float64
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
	// accepts 是历史成功总数， total 是历史总数
	accepts, total := b.history()
	weightedAccepts := b.k * float64(accepts)
	// https://landing.google.com/sre/sre-book/chapters/handling-overload/#eq2101
	dropRatio := math.Max(0, (float64(total-protection)-weightedAccepts)/float64(total+1))
	if dropRatio <= 0 {
		// 通过
		return nil
	}

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
