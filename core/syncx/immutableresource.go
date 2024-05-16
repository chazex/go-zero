package syncx

import (
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/timex"
)

// 定义了一个不可改变的资源（可以理解为一份缓存，一般用于某种不大码表）
// 但是里面有一点没看明白： refreshInterval定义了一个时间间隔，当超过这个时间，应当去刷新资源才对。但是并没有触发这个。
// 或者说这个refreshInterval的作用是这样的： 首先刚开始的时候，我们肯定要去拉取资源的，如果拉取到了，后面就一直用这个资源，永远不更新。
// 当我们拉取失败的时候，后面再获取资源时还是要拉取，refreshInterval可以避免我们总是频繁的拉取，限制我们1s拉取一次。

const defaultRefreshInterval = time.Second

type (
	// ImmutableResourceOption defines the method to customize an ImmutableResource.
	ImmutableResourceOption func(resource *ImmutableResource)

	// An ImmutableResource is used to manage an immutable resource.
	ImmutableResource struct {
		fetch           func() (any, error)
		resource        any
		err             error
		lock            sync.RWMutex
		refreshInterval time.Duration
		lastTime        *AtomicDuration
	}
)

// NewImmutableResource returns an ImmutableResource.
func NewImmutableResource(fn func() (any, error), opts ...ImmutableResourceOption) *ImmutableResource {
	// cannot use executors.LessExecutor because of cycle imports
	ir := ImmutableResource{
		fetch:           fn,
		refreshInterval: defaultRefreshInterval,
		lastTime:        NewAtomicDuration(),
	}
	for _, opt := range opts {
		opt(&ir)
	}
	return &ir
}

// Get gets the immutable resource, fetches automatically if not loaded.
func (ir *ImmutableResource) Get() (any, error) {
	ir.lock.RLock()
	resource := ir.resource
	ir.lock.RUnlock()
	if resource != nil {
		// 有资源直接返回
		return resource, nil
	}

	ir.maybeRefresh(func() {
		res, err := ir.fetch()
		ir.lock.Lock()
		if err != nil {
			ir.err = err
		} else {
			ir.resource, ir.err = res, nil
		}
		ir.lock.Unlock()
	})

	ir.lock.RLock()
	resource, err := ir.resource, ir.err
	ir.lock.RUnlock()
	return resource, err
}

func (ir *ImmutableResource) maybeRefresh(execute func()) {
	now := timex.Now()
	// 获取上次refresh时间
	lastTime := ir.lastTime.Load()
	// lastTime = 0 应该是初始值，第一次的时候，肯定要先拉取资源。
	if lastTime == 0 || lastTime+ir.refreshInterval < now {
		// 更新时间
		ir.lastTime.Set(now)
		// 更新资源
		execute()
	}
}

// WithRefreshIntervalOnFailure sets refresh interval on failure.
// Set interval to 0 to enforce refresh every time if not succeeded, default is time.Second.
func WithRefreshIntervalOnFailure(interval time.Duration) ImmutableResourceOption {
	return func(resource *ImmutableResource) {
		resource.refreshInterval = interval
	}
}
