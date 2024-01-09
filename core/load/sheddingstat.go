package load

import (
	"sync/atomic"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stat"
)

type (
	// A SheddingStat is used to store the statistics for load shedding.
	SheddingStat struct {
		name  string
		total int64
		pass  int64
		drop  int64
	}

	snapshot struct {
		Total int64
		Pass  int64
		Drop  int64
	}
)

//这个东西就是用在Schedding中间件，其他地方没用过

// NewSheddingStat returns a SheddingStat.
func NewSheddingStat(name string) *SheddingStat {
	st := &SheddingStat{
		name: name,
	}
	// 对外暴漏了统计total， pass， drop的接口，然后内部启动一个协程
	// 协程启动一个1分钟的ticker，每1分钟重置pass, drop,t total， 并打印cpu使用率， total, pass, drop的值
	// 这里的就是单纯的1分钟统计依次，并没有做滑动时间窗口
	go st.run()
	return st
}

// IncrementTotal increments the total requests.
func (s *SheddingStat) IncrementTotal() {
	atomic.AddInt64(&s.total, 1)
}

// IncrementPass increments the passed requests.
func (s *SheddingStat) IncrementPass() {
	atomic.AddInt64(&s.pass, 1)
}

// IncrementDrop increments the dropped requests.
func (s *SheddingStat) IncrementDrop() {
	atomic.AddInt64(&s.drop, 1)
}

func (s *SheddingStat) loop(c <-chan time.Time) {
	for range c {
		// pass, drop,t total 均置为0
		st := s.reset()

		if !logEnabled.True() {
			continue
		}

		c := stat.CpuUsage()
		if st.Drop == 0 {
			logx.Statf("(%s) shedding_stat [1m], cpu: %d, total: %d, pass: %d, drop: %d",
				s.name, c, st.Total, st.Pass, st.Drop)
		} else {
			logx.Statf("(%s) shedding_stat_drop [1m], cpu: %d, total: %d, pass: %d, drop: %d",
				s.name, c, st.Total, st.Pass, st.Drop)
		}
	}
}

func (s *SheddingStat) reset() snapshot {
	return snapshot{
		Total: atomic.SwapInt64(&s.total, 0),
		Pass:  atomic.SwapInt64(&s.pass, 0),
		Drop:  atomic.SwapInt64(&s.drop, 0),
	}
}

func (s *SheddingStat) run() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	s.loop(ticker.C)
}
