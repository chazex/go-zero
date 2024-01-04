package collection

import (
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/timex"
)

type (
	// RollingWindowOption let callers customize the RollingWindow.
	RollingWindowOption func(rollingWindow *RollingWindow)

	// RollingWindow defines a rolling window to calculate the events in buckets with time interval.
	RollingWindow struct {
		lock sync.RWMutex
		// 桶个数, 多个桶连起来就是完整的时间窗
		size int
		win  *window
		// 每个桶对应的时间范围
		interval time.Duration
		// 当前指向桶的索引号
		offset        int
		ignoreCurrent bool
		lastTime      time.Duration // start time of the last bucket
	}
)

// NewRollingWindow returns a RollingWindow that with size buckets and time interval,
// use opts to customize the RollingWindow.
func NewRollingWindow(size int, interval time.Duration, opts ...RollingWindowOption) *RollingWindow {
	if size < 1 {
		panic("size must be greater than 0")
	}

	w := &RollingWindow{
		size:     size,
		win:      newWindow(size),
		interval: interval,
		lastTime: timex.Now(),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Add adds value to current bucket.
func (rw *RollingWindow) Add(v float64) {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	// 滑动的动作发生在此
	rw.updateOffset()
	rw.win.add(rw.offset, v)
}

// Reduce函数的作用是，排除掉当前时间和上次时间之间的桶之后，把剩下的所有桶做统计

// Reduce runs fn on all buckets, ignore current bucket if ignoreCurrent was set.
func (rw *RollingWindow) Reduce(fn func(b *Bucket)) {
	rw.lock.RLock()
	defer rw.lock.RUnlock()

	var diff int
	span := rw.span()
	// ignore current bucket, because of partial data
	if span == 0 && rw.ignoreCurrent {
		diff = rw.size - 1
	} else {
		diff = rw.size - span
	}
	if diff > 0 {
		offset := (rw.offset + span + 1) % rw.size
		rw.win.reduce(offset, diff, fn)
	}
}

func (rw *RollingWindow) span() int {
	// offset  = 时间差 / 间隔, 此时offset代表，有多少个interval， offset >= 0， 并且小于size， 则直接使用offset， 否则使用size
	offset := int(timex.Since(rw.lastTime) / rw.interval)
	if 0 <= offset && offset < rw.size {
		return offset
	}

	// 超过桶个数直接返回桶个数
	return rw.size
}

func (rw *RollingWindow) updateOffset() {
	span := rw.span()
	if span <= 0 {
		return
	}

	offset := rw.offset
	// reset expired buckets
	for i := 0; i < span; i++ {
		rw.win.resetBucket((offset + i + 1) % rw.size)
	}

	// 重置offset
	rw.offset = (offset + span) % rw.size
	now := timex.Now()
	// 更新时间
	// align to interval time boundary
	rw.lastTime = now - (now-rw.lastTime)%rw.interval
}

// Bucket defines the bucket that holds sum and num of additions.
type Bucket struct {
	// 成功总数
	Sum float64
	// 成功 + 失败总数
	Count int64
}

func (b *Bucket) add(v float64) {
	b.Sum += v
	b.Count++
}

func (b *Bucket) reset() {
	b.Sum = 0
	b.Count = 0
}

// window对象，仅仅是一堆桶的集合，可以按照桶的编号对桶做一些操作，如清空桶，对桶里面的统计数据+1，做聚合等。
// 它本身并不做窗口滑动的操作。

type window struct {
	buckets []*Bucket
	size    int
}

func newWindow(size int) *window {
	buckets := make([]*Bucket, size)
	for i := 0; i < size; i++ {
		buckets[i] = new(Bucket)
	}
	return &window{
		buckets: buckets,
		size:    size,
	}
}

func (w *window) add(offset int, v float64) {
	// 往执行的 bucket 加入指定的指标
	w.buckets[offset%w.size].add(v)
}

// start 桶的开始索引
// count 桶的数量，即从start开始数几个桶
// start = 3, count = 10, 则从[3-12] % size
func (w *window) reduce(start, count int, fn func(b *Bucket)) {
	for i := 0; i < count; i++ {
		fn(w.buckets[(start+i)%w.size])
	}
}

func (w *window) resetBucket(offset int) {
	// 这里取余的原因是，担心给的offset太大，超过了w.size。取余就可以当作一个环来处理了。
	w.buckets[offset%w.size].reset()
}

// IgnoreCurrentBucket lets the Reduce call ignore current bucket.
func IgnoreCurrentBucket() RollingWindowOption {
	return func(w *RollingWindow) {
		w.ignoreCurrent = true
	}
}
