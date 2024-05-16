package breaker

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/mathx"
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/stringx"
)

const (
	numHistoryReasons = 5
	timeFormat        = "15:04:05"
)

// ErrServiceUnavailable is returned when the Breaker state is open.
var ErrServiceUnavailable = errors.New("circuit breaker is open")

type (
	// Acceptable is the func to check if the error can be accepted.
	Acceptable func(err error) bool

	// A Breaker represents a circuit breaker.
	Breaker interface {
		// Name returns the name of the Breaker.
		Name() string

		//Allow 判断请求是否被接受（是否被熔断），如果被接受，则返回一个Promise接口实例，Promise的Accept()方法，可以对请求成功做计数，Reject()方法可以对失败做计数， 成功和失败的计数用于后续的熔断判断.
		// 这种方法的使用场景为，先获取请求许可，并拿到Promise实例，然后执行业务逻辑，业务逻辑的成功和失败通过Promise对象的Accept和Reject方法来进行计数，从而实现了熔断的统计计算。

		// Allow checks if the request is allowed.
		// If allowed, a promise will be returned, the caller needs to call promise.Accept()
		// on success, or call promise.Reject() on failure.
		// If not allow, ErrServiceUnavailable will be returned.
		Allow() (Promise, error)

		// Do 也是获取请求许可执行业务逻辑，并依据业务逻辑结果做计数统计。 和Allow()的不一样的是，我们把业务逻辑通过回调方法的方式传过来。
		// 这样业务逻辑的执行，以及依据业务逻辑结果做计数统计的整个一套的处理流程由框架来做。

		// Do runs the given request if the Breaker accepts it.
		// Do returns an error instantly if the Breaker rejects the request.
		// If a panic occurs in the request, the Breaker handles it as an error
		// and causes the same panic again.
		Do(req func() error) error

		// 如果业务逻辑执行失败，返回error， 可以增加error回调函数，来判断哪些错误可以认为是可接受的，认为是成功。

		// DoWithAcceptable runs the given request if the Breaker accepts it.
		// DoWithAcceptable returns an error instantly if the Breaker rejects the request.
		// If a panic occurs in the request, the Breaker handles it as an error
		// and causes the same panic again.
		// acceptable checks if it's a successful call, even if the error is not nil.
		DoWithAcceptable(req func() error, acceptable Acceptable) error

		// fallback被熔断器熔断时，调用的回调函数

		// DoWithFallback runs the given request if the Breaker accepts it.
		// DoWithFallback runs the fallback if the Breaker rejects the request.
		// If a panic occurs in the request, the Breaker handles it as an error
		// and causes the same panic again.
		DoWithFallback(req func() error, fallback func(err error) error) error

		// DoWithFallbackAcceptable runs the given request if the Breaker accepts it.
		// DoWithFallbackAcceptable runs the fallback if the Breaker rejects the request.
		// If a panic occurs in the request, the Breaker handles it as an error
		// and causes the same panic again.
		// acceptable checks if it's a successful call, even if the error is not nil.
		DoWithFallbackAcceptable(req func() error, fallback func(err error) error, acceptable Acceptable) error
	}

	// Option defines the method to customize a Breaker.
	Option func(breaker *circuitBreaker)

	//=====================================================================================
	// 本文件中，作者用了很多的封装思想。某一功能，会出现两类接口。 Xxx和internalXxx, 比如Promise 和 internalPromise, 以及 throttle 和 internalThrottle
	// internalXXX类型的接口，它的实现和实际功能相关，在本源码中，是和熔断相关的，具体来讲就是googlePromise 和 googleBreaker
	// XXX 类型的接口的实现， 它是对internalXXX接口实现的封装， 内部实际上调用的是还是internalXXX接口的实现，它在外围做了一些记录日志，统计，记录额外信息的功能。
	// XXX 类型的接口 是对外暴漏给用户使用的， internalXXX类型的接口，是在内部使用的。

	// 目前Promise接口，只有一个有效的实现，就是promiseWithReason， 它是对internalPromise的封装。promiseWithReason会在外围做一些记录近几次的reject原因。 对于Accept和Reject调用的其实是
	// internalPromise的实现， googlePromise

	// Promise interface defines the callbacks that returned by Breaker.Allow.
	Promise interface {
		// Accept tells the Breaker that the call is successful.
		Accept()
		// Reject tells the Breaker that the call is failed.
		Reject(reason string)
	}

	internalPromise interface {
		Accept()
		Reject()
	}

	circuitBreaker struct {
		name string
		throttle
	}

	internalThrottle interface {
		allow() (internalPromise, error)
		doReq(req func() error, fallback func(err error) error, acceptable Acceptable) error
	}

	// throttle 是对interface internalThrottle的封装，internalThrottle是实际的熔断逻辑， throttle 会在外围做一些统计、日志相关的工作。
	// throttle 的实现是loggedThrottle， 从名字也能看出来，是记录日志的。  internalThrottle的实现是googleBreaker， 所以它才是真正的熔断逻辑。
	throttle interface {
		allow() (Promise, error)
		doReq(req func() error, fallback func(err error) error, acceptable Acceptable) error
	}
)

// NewBreaker returns a Breaker object.
// opts can be used to customize the Breaker.
func NewBreaker(opts ...Option) Breaker {
	var b circuitBreaker
	for _, opt := range opts {
		opt(&b)
	}
	// 可以通过参数Option指定名字，如果没指定，则随机生成字符串
	if len(b.name) == 0 {
		b.name = stringx.Rand()
	}
	b.throttle = newLoggedThrottle(b.name, newGoogleBreaker())

	return &b
}

func (cb *circuitBreaker) Allow() (Promise, error) {
	return cb.throttle.allow()
}

func (cb *circuitBreaker) Do(req func() error) error {
	return cb.throttle.doReq(req, nil, defaultAcceptable)
}

func (cb *circuitBreaker) DoWithAcceptable(req func() error, acceptable Acceptable) error {
	return cb.throttle.doReq(req, nil, acceptable)
}

func (cb *circuitBreaker) DoWithFallback(req func() error, fallback func(err error) error) error {
	return cb.throttle.doReq(req, fallback, defaultAcceptable)
}

func (cb *circuitBreaker) DoWithFallbackAcceptable(req func() error, fallback func(err error) error,
	acceptable Acceptable) error {
	return cb.throttle.doReq(req, fallback, acceptable)
}

func (cb *circuitBreaker) Name() string {
	return cb.name
}

// WithName returns a function to set the name of a Breaker.
func WithName(name string) Option {
	return func(b *circuitBreaker) {
		b.name = name
	}
}

func defaultAcceptable(err error) bool {
	return err == nil
}

// loggedThrottle 是对interface throttle的实现，他内部实际上调用的是internalThrottle。
// internalThrottle 接口是实际处理熔断判断的逻辑。 loggedThrottle 在外层做了一些统计，日志的工作。
type loggedThrottle struct {
	name string
	internalThrottle
	errWin *errorWindow
}

func newLoggedThrottle(name string, t internalThrottle) loggedThrottle {
	return loggedThrottle{
		name:             name,
		internalThrottle: t,
		errWin:           new(errorWindow),
	}
}

func (lt loggedThrottle) allow() (Promise, error) {
	promise, err := lt.internalThrottle.allow()
	return promiseWithReason{
		promise: promise,
		errWin:  lt.errWin,
	}, lt.logError(err)
}

func (lt loggedThrottle) doReq(req func() error, fallback func(err error) error, acceptable Acceptable) error {
	return lt.logError(lt.internalThrottle.doReq(req, fallback, func(err error) bool {
		accept := acceptable(err)
		if !accept && err != nil {
			lt.errWin.add(err.Error())
		}
		return accept
	}))
}

func (lt loggedThrottle) logError(err error) error {
	// 如果错误是由熔断引起的，则日志记录熔断相关的信息。
	if errors.Is(err, ErrServiceUnavailable) {
		// if circuit open, not possible to have empty error window
		stat.Report(fmt.Sprintf(
			"proc(%s/%d), callee: %s, breaker is open and requests dropped\nlast errors:\n%s",
			proc.ProcessName(), proc.Pid(), lt.name, lt.errWin))
	}

	return err
}

type errorWindow struct {
	reasons [numHistoryReasons]string
	// index  window 的下一个可写的索引
	index int
	count int
	lock  sync.Mutex
}

func (ew *errorWindow) add(reason string) {
	ew.lock.Lock()
	ew.reasons[ew.index] = fmt.Sprintf("%s %s", time.Now().Format(timeFormat), reason)
	ew.index = (ew.index + 1) % numHistoryReasons
	// 这里貌似执行一段时间后，就一直都是 numHistoryReasons 了。
	ew.count = mathx.MinInt(ew.count+1, numHistoryReasons)
	ew.lock.Unlock()
}

// String 逆序拼接所有原因，形成字符串
func (ew *errorWindow) String() string {
	var reasons []string

	ew.lock.Lock()
	// reverse order
	for i := ew.index - 1; i >= ew.index-ew.count; i-- {
		reasons = append(reasons, ew.reasons[(i+numHistoryReasons)%numHistoryReasons])
	}
	ew.lock.Unlock()

	return strings.Join(reasons, "\n")
}

// Promise interface的实现， 同时也是对internalPromise的封装。
// 内部逻辑实际上调用的是internalPromise，promiseWithReason在外围记录了Reject的近几次的原因。
type promiseWithReason struct {
	promise internalPromise
	errWin  *errorWindow
}

func (p promiseWithReason) Accept() {
	p.promise.Accept()
}

func (p promiseWithReason) Reject(reason string) {
	// 记录近几次的错误
	p.errWin.add(reason)
	p.promise.Reject()
}
