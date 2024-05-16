package threading

import (
	"github.com/zeromicro/go-zero/core/lang"
	"github.com/zeromicro/go-zero/core/rescue"
)

// 控制并发的goroutine数量的结构。
// 原理: 初始化一个有容量的channel，channel中存储的内容可以认为是token， channel的容量，即使token的数量，也代表的即使允许并发的数量
// 通过Schedule 运行一个goroutine， 运行之前首先从channel拿到token，如果拿不到则等待。

// A TaskRunner is used to control the concurrency of goroutines.
type TaskRunner struct {
	limitChan chan lang.PlaceholderType
}

// NewTaskRunner returns a TaskRunner.
func NewTaskRunner(concurrency int) *TaskRunner {
	return &TaskRunner{
		limitChan: make(chan lang.PlaceholderType, concurrency),
	}
}

// Schedule schedules a task to run under concurrency control.
func (rp *TaskRunner) Schedule(task func()) {
	rp.limitChan <- lang.Placeholder

	go func() {
		defer rescue.Recover(func() {
			<-rp.limitChan
		})

		task()
	}()
}
