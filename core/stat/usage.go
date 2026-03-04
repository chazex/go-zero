package stat

import (
	"github.com/zeromicro/go-zero/core/stat/internal"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/threading"
)

const (
	// 250ms and 0.95 as beta will count the average cpu load for past 5 seconds
	cpuRefreshInterval = time.Millisecond * 250
	allRefreshInterval = time.Minute
	// moving average beta hyperparameter
	beta = 0.95
)

var cpuUsage int64

// https://www.cnblogs.com/kevinwan/p/13857358.html
// https://zhuanlan.zhihu.com/p/684148558
func init() {
	go func() {
		cpuTicker := time.NewTicker(cpuRefreshInterval)
		defer cpuTicker.Stop()
		allTicker := time.NewTicker(allRefreshInterval)
		defer allTicker.Stop()

		for {
			select {
			case <-cpuTicker.C:
				// 定时启动协程，刷新cpu使用率
				threading.RunSafe(func() {
					// curUsage 也是一段时间的值
					curUsage := internal.RefreshCpu()
					prevUsage := atomic.LoadInt64(&cpuUsage)
					// 滑动平均 或者叫做指数加权平均 ： https://www.cnblogs.com/wuliytTaotao/p/9479958.html
					// cput-1指的是t-1时刻的cpu使用率， cput指的是t时刻的cpu使用率。 这里的beta值为0，95，则表示老的cpu使用率占比95%， 新的cpu使用率占比5%，这是为什么呢？
					// cpu = cpuᵗ⁻¹ * beta + cpuᵗ * (1 - beta)
					usage := int64(float64(prevUsage)*beta + float64(curUsage)*(1-beta))
					atomic.StoreInt64(&cpuUsage, usage)
				})
			case <-allTicker.C:
				// 定时打印日志， CPU 内存等
				if logEnabled.True() {
					printUsage()
				}
			}
		}
	}()
}

// CpuUsage returns current cpu usage.
func CpuUsage() int64 {
	return atomic.LoadInt64(&cpuUsage)
}

func bToMb(b uint64) float32 {
	return float32(b) / 1024 / 1024
}

func printUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	logx.Statf("CPU: %dm, MEMORY: Alloc=%.1fMi, TotalAlloc=%.1fMi, Sys=%.1fMi, NumGC=%d",
		CpuUsage(), bToMb(m.Alloc), bToMb(m.TotalAlloc), bToMb(m.Sys), m.NumGC)
}
