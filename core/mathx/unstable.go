package mathx

import (
	"math/rand"
	"sync"
	"time"
)

// Unstable 用于根据给定偏差生成平均值周围的随机值。
// 在模拟随机波动等场景中非常有用。
// 最终生成的值的范围在 base * [1 - u.deviation, 1 + u.deviation]之内波动
// 如果 base = 100 且 u.deviation = 0.2,那么生成的随机值范围就是 80 到 120。

// An Unstable is used to generate random value around the mean value base on given deviation.
type Unstable struct {
	deviation float64
	r         *rand.Rand
	lock      *sync.Mutex
}

// NewUnstable returns an Unstable.
func NewUnstable(deviation float64) Unstable {
	if deviation < 0 {
		deviation = 0
	}
	if deviation > 1 {
		deviation = 1
	}
	return Unstable{
		deviation: deviation,
		r:         rand.New(rand.NewSource(time.Now().UnixNano())),
		lock:      new(sync.Mutex),
	}
}

/**
首先,这个公式的目的是生成一个以 base 为中心,在一定范围内波动的随机值。其中 u.deviation 控制波动的幅度,取值范围是 (0, 1)。
我们可以将这个公式拆解为两部分:

1.  1 + u.deviation - 2*u.deviation*u.r.Float64()
2.  float64(base)

第一部分
	1 + u.deviation - 2*u.deviation*u.r.Float64() 用于生成一个在 [1 - u.deviation, 1 + u.deviation] 范围内的随机系数。具体来说:

	1 + u.deviation - 2*u.deviation*u.r.Float64() 等价于 (1 + u.deviation) - (2*u.deviation*u.r.Float64())。
	u.r.Float64() 生成一个 [0, 1) 范围内的随机浮点数。
	2*u.deviation*u.r.Float64() 将这个随机浮点数映射到 [0, 2*u.deviation) 范围内。因为u.r.Float64() 的取值范围是 [0, 1),所以 2*u.deviation*u.r.Float64() 的取值范围是 [0, 2*u.deviation)
	那么 1 + u.deviation - 2*u.deviation*u.r.Float64() 的取值范围就是
	[(1 + u.deviation) - (2*u.deviation), 1 + u.deviation],也就是 [1 - u.deviation, 1 + u.deviation]。
第二部分
	float64(base) 是将给定的基准值 base 转换为浮点数类型,以便与第一部分的系数相乘。
	将两部分相乘,我们就得到了一个随机值,它的范围是:
	(1 - u.deviation) * float64(base) 到 (1 + u.deviation) * float64(base)

例如,如果 base = 100 且 u.deviation = 0.2,那么生成的随机值范围就是 80 到 120。
通过调整 u.deviation 的值,我们可以控制随机值波动的幅度。当 u.deviation = 0 时,生成的随机值就等于 base。当 u.deviation 越大,生成的随机值波动范围也就越大。
总的来说,这个公式利用了随机数生成和线性映射的方法,能够生成一个以给定基准值为中心,在一定范围内波动的随机值。这种方法在模拟随机波动等场景中非常有用。
*/

// AroundDuration returns a random duration with given base and deviation.
func (u Unstable) AroundDuration(base time.Duration) time.Duration {
	u.lock.Lock()
	val := time.Duration((1 + u.deviation - 2*u.deviation*u.r.Float64()) * float64(base))
	u.lock.Unlock()
	return val
}

// AroundInt returns a random int64 with given base and deviation.
func (u Unstable) AroundInt(base int64) int64 {
	u.lock.Lock()
	val := int64((1 + u.deviation - 2*u.deviation*u.r.Float64()) * float64(base))
	u.lock.Unlock()
	return val
}
