//go:build linux || darwin

package proc

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/threading"
)

const (
	wrapUpTime = time.Second
	// why we use 5500 milliseconds is because most of our queue are blocking mode with 5 seconds
	waitTime = 5500 * time.Millisecond
)

var (
	wrapUpListeners          = new(listenerManager)
	shutdownListeners        = new(listenerManager)
	delayTimeBeforeForceQuit = waitTime
)

// AddShutdownListener adds fn as a shutdown listener.
// The returned func can be used to wait for fn getting called.
func AddShutdownListener(fn func()) (waitForCalled func()) {
	// 添加监听函数，返回值也是一个函数，waitForCalled()，调用此函数会阻塞，直到列表里面所有的监听hd
	return shutdownListeners.addListener(fn)
}

// AddWrapUpListener adds fn as a wrap up listener.
// The returned func can be used to wait for fn getting called.
func AddWrapUpListener(fn func()) (waitForCalled func()) {
	return wrapUpListeners.addListener(fn)
}

// SetTimeToForceQuit sets the waiting time before force quitting.
func SetTimeToForceQuit(duration time.Duration) {
	delayTimeBeforeForceQuit = duration
}

//func gracefulStop(signals chan os.Signal) {
//	signal.Stop(signals) //停止channel signals接收信号
//
//	logx.Info("Got signal SIGTERM, shutting down...")
//	go wrapUpListeners.notifyListeners() // 异步执行注册的监听函数
//
//	time.Sleep(wrapUpTime)
//	go shutdownListeners.notifyListeners() // 异步执行注册的监听函数
//
//	// 此时如果注册的监听函数正常执行完成，主goroutine应该已经退出
//
//	// 等待一段时间后，依然没有退出，则强制退出
//	time.Sleep(delayTimeBeforeForceQuit - wrapUpTime)
//	logx.Infof("Still alive after %v, going to force kill the process...", delayTimeBeforeForceQuit)
//	syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
//}

// Shutdown calls the registered shutdown listeners, only for test purpose.
func Shutdown() {
	shutdownListeners.notifyListeners()
}

// WrapUp wraps up the process, only for test purpose.
func WrapUp() {
	wrapUpListeners.notifyListeners()
}

func gracefulStop(signals chan os.Signal, sig syscall.Signal) {
	signal.Stop(signals)

	logx.Infof("Got signal %d, shutting down...", sig)
	go wrapUpListeners.notifyListeners()

	time.Sleep(wrapUpTime)
	go shutdownListeners.notifyListeners() // 异步执行注册的监听函数

	// 此时如果注册的监听函数正常执行完成，主goroutine应该已经退出

	// 等待一段时间后，依然没有退出，则强制退出
	time.Sleep(delayTimeBeforeForceQuit - wrapUpTime)
	logx.Infof("Still alive after %v, going to force kill the process...", delayTimeBeforeForceQuit)
	_ = syscall.Kill(syscall.Getpid(), sig)
}

type listenerManager struct {
	lock      sync.Mutex
	waitGroup sync.WaitGroup
	listeners []func()
}

func (lm *listenerManager) addListener(fn func()) (waitForCalled func()) {
	lm.waitGroup.Add(1)

	lm.lock.Lock()
	lm.listeners = append(lm.listeners, func() {
		defer lm.waitGroup.Done()
		fn()
	})
	lm.lock.Unlock()

	return func() {
		lm.waitGroup.Wait()
	}
}

func (lm *listenerManager) notifyListeners() {
	lm.lock.Lock()
	defer lm.lock.Unlock()

	group := threading.NewRoutineGroup()
	for _, listener := range lm.listeners {
		group.RunSafe(listener)
	}
	group.Wait()

	lm.listeners = nil
}
