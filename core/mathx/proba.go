package mathx

import (
	"math/rand"
	"sync"
	"time"
)

// A Proba is used to test if true on given probability.
type Proba struct {
	// rand.New(...) returns a non thread safe object
	r    *rand.Rand
	lock sync.Mutex
}

// NewProba returns a Proba.
func NewProba() *Proba {
	return &Proba{
		// 使用当前时间的纳秒值作为种子来创建一个新的随机数源。
		r: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// TrueOnProba checks if true on given probability.
func (p *Proba) TrueOnProba(proba float64) (truth bool) {
	p.lock.Lock()
	// p.r.Float64()返回 [0.0,1.0) 之间的一个为随机数
	// 通过我们传入的参数proba， 和生成的随机数比大小。 如果随机数小，返回true，否则返回false
	truth = p.r.Float64() < proba
	p.lock.Unlock()
	return
}
