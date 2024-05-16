package collection

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/mathx"
	"github.com/zeromicro/go-zero/core/syncx"
)

const (
	defaultCacheName = "proc"
	slots            = 300
	statInterval     = time.Minute
	// make the expiry unstable to avoid lots of cached items expire at the same time
	// make the unstable expiry to be [0.95, 1.05] * seconds
	expiryDeviation = 0.05
)

var emptyLruCache = emptyLru{}

type (
	// CacheOption defines the method to customize a Cache.
	CacheOption func(cache *Cache)

	// 本地缓存， 支持将数据存储到内存中。在缓存miss的情况下，支持从源程拉取。
	// 缓存支持过期时间， 过期时间通过时间轮实现。
	// 内部可使用lru，来保证本地缓存不会过多，会定期删除，同时支持过期删除的回调函数。

	// A Cache object is an in-memory cache.
	Cache struct {
		name string
		lock sync.Mutex
		// 缓存的数据存储位置
		data map[string]any
		// key的过期时间
		expire time.Duration
		// 时间轮，用于对过期的key做删除。
		timingWheel *TimingWheel
		// lru，根据key的添加的早晚，来排序。
		lruCache       lru
		barrier        syncx.SingleFlight
		unstableExpiry mathx.Unstable
		stats          *cacheStat
	}
)

// NewCache returns a Cache with given expire.
func NewCache(expire time.Duration, opts ...CacheOption) (*Cache, error) {
	cache := &Cache{
		data:           make(map[string]any),
		expire:         expire,
		lruCache:       emptyLruCache,
		barrier:        syncx.NewSingleFlight(),
		unstableExpiry: mathx.NewUnstable(expiryDeviation), // 过期时间的波动范围
	}

	for _, opt := range opts {
		opt(cache)
	}

	if len(cache.name) == 0 {
		cache.name = defaultCacheName
	}
	cache.stats = newCacheStat(cache.name, cache.size)

	timingWheel, err := NewTimingWheel(time.Second, slots, func(k, v any) {
		key, ok := k.(string)
		if !ok {
			return
		}

		cache.Del(key)
	})
	if err != nil {
		return nil, err
	}

	cache.timingWheel = timingWheel
	return cache, nil
}

// Del 根据key删除缓存。
// Del deletes the item with the given key from c.
func (c *Cache) Del(key string) {
	c.lock.Lock()
	// 删除缓存
	delete(c.data, key)
	// 从lru中删除（lru不存储缓存的具体值，只存储key，并且key是按照先后排序的）
	c.lruCache.remove(key)
	c.lock.Unlock()
	// 删除时间轮中的缓存过期清理任务
	c.timingWheel.RemoveTimer(key)
}

// Get returns the item with the given key from c.
func (c *Cache) Get(key string) (any, bool) {
	// 获取一个key
	value, ok := c.doGet(key)

	// 统计缓存的命中和miss情况
	if ok {
		c.stats.IncrementHit()
	} else {
		c.stats.IncrementMiss()
	}

	return value, ok
}

// Set sets value into c with key.
func (c *Cache) Set(key string, value any) {
	c.SetWithExpire(key, value, c.expire)
}

// SetWithExpire sets value into c with key and expire with the given value.
func (c *Cache) SetWithExpire(key string, value any, expire time.Duration) {
	c.lock.Lock()
	_, ok := c.data[key]
	c.data[key] = value
	c.lruCache.add(key)
	c.lock.Unlock()

	// 用户给出的过期时间，不直接使用，而是依据此值，做一些波动。避免统一时刻大量过期。
	expiry := c.unstableExpiry.AroundDuration(expire)
	if ok {
		// 如果key之前就存在，那么需要通过时间轮来修改过期时间
		c.timingWheel.MoveTimer(key, expiry)
	} else {
		// key之前不存在，通过时间轮新加一个过期任务。
		c.timingWheel.SetTimer(key, value, expiry)
	}
}

// Take returns the item with the given key.
// If the item is in c, return it directly.
// If not, use fetch method to get the item, set into c and return it.
func (c *Cache) Take(key string, fetch func() (any, error)) (any, error) {
	if val, ok := c.doGet(key); ok {
		c.stats.IncrementHit()
		return val, nil
	}

	// 表示是从本地直接拿到了，还是从远程拿到的。
	var fresh bool
	val, err := c.barrier.Do(key, func() (any, error) {
		// because O(1) on map search in memory, and fetch is an IO query
		// so we do double check, cache might be taken by another call
		if val, ok := c.doGet(key); ok {
			return val, nil
		}

		// 本地缓存没有，则从别处拉取（比如从db，或者http服务等）
		v, e := fetch()
		if e != nil {
			return nil, e
		}

		fresh = true
		c.Set(key, v)
		return v, nil
	})
	if err != nil {
		return nil, err
	}

	if fresh {
		// 从源程拿到的，说明本地缓存miss了。
		c.stats.IncrementMiss()
		return val, nil
	}

	// got the result from previous ongoing query
	c.stats.IncrementHit()
	return val, nil
}

func (c *Cache) doGet(key string) (any, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	value, ok := c.data[key]
	if ok {
		// 这里调用的是lru的add()方法，这里的add实际上有两种情况，1. add。 2. 更新key的入缓存“时间”
		// 这里实际上是第二种情况，因为获取一个key，就代表这个key最近被用过，所以需要认为它是比较新的。
		c.lruCache.add(key)
	}

	return value, ok
}

func (c *Cache) onEvict(key string) {
	// already locked
	delete(c.data, key)
	c.timingWheel.RemoveTimer(key)
}

func (c *Cache) size() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return len(c.data)
}

// WithLimit customizes a Cache with items up to limit.
func WithLimit(limit int) CacheOption {
	return func(cache *Cache) {
		if limit > 0 {
			cache.lruCache = newKeyLru(limit, cache.onEvict)
		}
	}
}

// WithName customizes a Cache with the given name.
func WithName(name string) CacheOption {
	return func(cache *Cache) {
		cache.name = name
	}
}

type (
	lru interface {
		add(key string)
		remove(key string)
	}

	emptyLru struct{}

	// keyLru 内部利用链表，来实现了Lru。
	// 其原理为：key不存在时，插入到链表的头部，如果key已经存在了（在链表中已有），则把元素移到链表的头部。
	// 这样删除时，就可以从链表的尾部，把最老的删掉。
	keyLru struct {
		limit  int
		evicts *list.List
		// 一个map，用于判断key是否存在，以及根据key删除时，可以快速获取到链表中的某个节点。
		elements map[string]*list.Element
		// key被移除时的回调。
		onEvict func(key string)
	}
)

func (elru emptyLru) add(string) {
}

func (elru emptyLru) remove(string) {
}

func newKeyLru(limit int, onEvict func(key string)) *keyLru {
	return &keyLru{
		limit:    limit,
		evicts:   list.New(),
		elements: make(map[string]*list.Element),
		onEvict:  onEvict,
	}
}

func (klru *keyLru) add(key string) {
	if elem, ok := klru.elements[key]; ok {
		// 如果已经存在了，则移动到list的头上。
		klru.evicts.MoveToFront(elem)
		return
	}

	// 在链表的头上添加新节点，并在map中保存
	// Add new item
	elem := klru.evicts.PushFront(key)
	klru.elements[key] = elem

	// 如果长度超过限制，则删除
	// Verify size not exceeded
	if klru.evicts.Len() > klru.limit {
		klru.removeOldest()
	}
}

// 根据指定的key删除
func (klru *keyLru) remove(key string) {
	if elem, ok := klru.elements[key]; ok {
		klru.removeElement(elem)
	}
}

// 移除最老的，也就是链表的最尾部的
func (klru *keyLru) removeOldest() {
	// 获取尾部节点
	elem := klru.evicts.Back()
	if elem != nil {
		klru.removeElement(elem)
	}
}

func (klru *keyLru) removeElement(e *list.Element) {
	// 从链表中移除
	klru.evicts.Remove(e)
	key := e.Value.(string)
	// 从map中移除
	delete(klru.elements, key)
	// 调用移除回调函数
	klru.onEvict(key)
}

type cacheStat struct {
	name         string
	hit          uint64
	miss         uint64
	sizeCallback func() int
}

// 统计一段时间内的，缓存情况，包括请求总数，命中数，miss数，命中率，缓存中key数量等。
// 并且会将数据定期输出到日志中。
func newCacheStat(name string, sizeCallback func() int) *cacheStat {
	st := &cacheStat{
		name:         name,
		sizeCallback: sizeCallback,
	}
	go st.statLoop()
	return st
}

func (cs *cacheStat) IncrementHit() {
	atomic.AddUint64(&cs.hit, 1)
}

func (cs *cacheStat) IncrementMiss() {
	atomic.AddUint64(&cs.miss, 1)
}

func (cs *cacheStat) statLoop() {
	ticker := time.NewTicker(statInterval)
	defer ticker.Stop()

	for range ticker.C {
		// 清零
		hit := atomic.SwapUint64(&cs.hit, 0)
		miss := atomic.SwapUint64(&cs.miss, 0)
		total := hit + miss
		if total == 0 {
			continue
		}
		percent := 100 * float32(hit) / float32(total)
		//					缓存名称			请求总数		命中率				元素数量	  请求命中数	请求miss数
		logx.Statf("cache(%s) - qpm: %d, hit_ratio: %.1f%%, elements: %d, hit: %d, miss: %d",
			cs.name, total, percent, cs.sizeCallback(), hit, miss)
	}
}
