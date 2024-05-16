package syncx

import (
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/timex"
)

// 创建一个资源池。 资源池使用一个链表，将所有的资源链接起来。
// 每次添加资源到资源池中，会放到链表的头中。 资源支持过期，但是过期检测时懒检测。
// 资源池有最大值，当从资源池获取资源时，如果池中没有资源，并且已创建的资源没超过最大值，则会创建新的资源并返回。
// 如果已创建的资源超过最大值，则会阻塞等待，直到获取到资源为止（资源归还）。

type (
	// PoolOption defines the method to customize a Pool.
	PoolOption func(*Pool)

	node struct {
		item     any
		next     *node
		lastUsed time.Duration
	}

	// A Pool is used to pool resources.
	// The difference between sync.Pool is that:
	//  1. the limit of the resources
	//  2. max age of the resources can be set
	//  3. the method to destroy resources can be customized
	Pool struct {
		limit   int
		created int
		maxAge  time.Duration
		lock    sync.Locker
		cond    *sync.Cond
		head    *node
		create  func() any
		destroy func(any)
	}
)

// NewPool returns a Pool.
func NewPool(n int, create func() any, destroy func(any), opts ...PoolOption) *Pool {
	if n <= 0 {
		panic("pool size can't be negative or zero")
	}

	lock := new(sync.Mutex)
	pool := &Pool{
		limit:   n,
		lock:    lock,
		cond:    sync.NewCond(lock),
		create:  create,
		destroy: destroy,
	}

	for _, opt := range opts {
		opt(pool)
	}

	return pool
}

// Get gets a resource.
func (p *Pool) Get() any {
	p.lock.Lock()
	defer p.lock.Unlock()

	for {
		if p.head != nil {
			head := p.head
			p.head = head.next
			// 延迟关闭
			if p.maxAge > 0 && head.lastUsed+p.maxAge < timex.Now() {
				// 已过期
				p.created--
				p.destroy(head.item)
				continue
			} else {
				return head.item
			}
		}

		// 如果pool中没有可用的资源，并且数量小于limit，此时新创建
		if p.created < p.limit {
			p.created++
			return p.create()
		}

		// 如果pool中没有可用资源，并且数量已经达到上线，需要等待put函数的通知，put函数会将资源放到pool中，然后发出信号，等待获取资源的就可以尝试拿资源了。
		p.cond.Wait()
	}
}

// Put puts a resource back.
func (p *Pool) Put(x any) {
	if x == nil {
		return
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	// 向资源池中添加资源。
	p.head = &node{
		item:     x,
		next:     p.head,
		lastUsed: timex.Now(),
	}
	// 发出资源可用信号
	p.cond.Signal()
}

// WithMaxAge returns a function to customize a Pool with given max age.
func WithMaxAge(duration time.Duration) PoolOption {
	return func(pool *Pool) {
		pool.maxAge = duration
	}
}
