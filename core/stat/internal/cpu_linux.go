package internal

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/iox"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	cpuTicks  = 100
	cpuFields = 8
)

var (
	preSystem uint64
	preTotal  uint64
	quota     float64
	cores     uint64
	initOnce  sync.Once
)

// if /proc not present, ignore the cpu calculation, like wsl linux
func initialize() {
	// 获取cpu核数
	cpus, err := cpuSets()
	if err != nil {
		logx.Error(err)
		return
	}

	// cpu核数
	cores = uint64(len(cpus))
	// cpu配额
	quota = float64(len(cpus))
	// 如果为-1，则表示为没有限制，此时cores = quota
	// 如果不为-1， 则需要重新计算，limit，
	cq, err := cpuQuota()
	if err == nil {
		if cq != -1 {
			period, err := cpuPeriod()
			if err != nil {
				logx.Error(err)
				return
			}

			limit := float64(cq) / float64(period)
			if limit < quota {
				quota = limit
			}
		}
	}

	// 因为计算cpu使用率，是计算一段时间内的，所以需要用 （当前值 - 老值）， 这里初始化的时候，为了不让老值为0， 所以需要先算一下，用来作为老值。
	// 这里算出来的值是 瞬时的。
	preSystem, err = systemCpuUsage()
	if err != nil {
		logx.Error(err)
		return
	}

	preTotal, err = totalCpuUsage()
	if err != nil {
		logx.Error(err)
		return
	}
}

// RefreshCpu refreshes cpu usage and returns.
func RefreshCpu() uint64 {
	initOnce.Do(initialize)

	total, err := totalCpuUsage()
	if err != nil {
		return 0
	}

	system, err := systemCpuUsage()
	if err != nil {
		return 0
	}

	var usage uint64
	cpuDelta := total - preTotal
	systemDelta := system - preSystem
	if cpuDelta > 0 && systemDelta > 0 {
		// 1e3是科学计数法， 值位1000
		usage = uint64(float64(cpuDelta*cores*1e3) / (float64(systemDelta) * quota))
	}
	preSystem = system
	preTotal = total

	return usage
}

// cgruop v1 查找文件 cat /sys/fs/cgroup/cpu/cpu.cfs_quota_us -1 表示没限制
func cpuQuota() (int64, error) {
	cg, err := currentCgroup()
	if err != nil {
		return 0, err
	}

	return cg.cpuQuotaUs()
}

// cgroup v1 查看文件  cat /sys/fs/cgroup/cpu/cpu.cfs_period_us
func cpuPeriod() (uint64, error) {
	cg, err := currentCgroup()
	if err != nil {
		return 0, err
	}

	return cg.cpuPeriodUs()
}

func cpuSets() ([]uint64, error) {
	cg, err := currentCgroup()
	if err != nil {
		return nil, err
	}

	return cg.cpus()
}

func systemCpuUsage() (uint64, error) {
	// 读取 /proc/stat 文件的内容，并去除空行
	lines, err := iox.ReadTextLines("/proc/stat", iox.WithoutBlank())
	if err != nil {
		return 0, err
	}

	// 遍历每一行
	for _, line := range lines {
		// 以空格为分隔符将行拆分成字段
		fields := strings.Fields(line)
		// 找到以 "cpu" 开头的行
		if fields[0] == "cpu" {
			if len(fields) < cpuFields {
				return 0, fmt.Errorf("bad format of cpu stats")
			}

			var totalClockTicks uint64
			for _, i := range fields[1:cpuFields] {
				// parseUint就是将字符串转换为uint，内部处理有些错误
				v, err := parseUint(i)
				if err != nil {
					return 0, err
				}

				// 计算总的时钟周期数
				totalClockTicks += v
			}

			return (totalClockTicks * uint64(time.Second)) / cpuTicks, nil
		}
	}

	return 0, errors.New("bad stats format")
}

func totalCpuUsage() (usage uint64, err error) {
	var cg cgroup
	if cg, err = currentCgroup(); err != nil {
		return
	}

	return cg.usageAllCpus()
}
