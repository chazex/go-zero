package internal

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/internal/health"
)

const probeNamePrefix = "rest"

// StartOption defines the method to customize http.Server.
type StartOption func(svr *http.Server)

// StartHttp starts a http server.
func StartHttp(host string, port int, handler http.Handler, opts ...StartOption) error {
	return start(host, port, handler, func(svr *http.Server) error {
		return svr.ListenAndServe()
	}, opts...)
}

// StartHttps starts a https server.
func StartHttps(host string, port int, certFile, keyFile string, handler http.Handler,
	opts ...StartOption) error {
	return start(host, port, handler, func(svr *http.Server) error {
		// certFile and keyFile are set in buildHttpsServer
		return svr.ListenAndServeTLS(certFile, keyFile)
	}, opts...)
}

func start(host string, port int, handler http.Handler, run func(svr *http.Server) error,
	opts ...StartOption) (err error) {
	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", host, port),
		Handler: handler,
	}
	for _, opt := range opts {
		opt(server)
	}

	// 创建一个探针
	healthManager := health.NewHealthManager(fmt.Sprintf("%s-%s:%d", probeNamePrefix, host, port))

	waitForCalled := proc.AddShutdownListener(func() {
		// 当系统收到 退出信号的时候， 会执行我们注册的函数。
		// 由于要退出，所以需要标记为NotReady。 作用： 假如使用了注册中心，此服务向注册中心注册，注册中心会不停的轮询我们服务的状态， 当拿到状态为不是ready，那么就会把这个服务摘掉了。
		healthManager.MarkNotReady()
		// 然后停止HTTP服务
		if e := server.Shutdown(context.Background()); e != nil {
			logx.Error(e)
		}
	})
	defer func() {
		// http.ErrServerClosed 是HTTP服务正常关闭的一个标志，其实不算是一个错误。
		if errors.Is(err, http.ErrServerClosed) {
			// HTTP服务正常关闭后，我们要等待ShutdownListener中的其他的监听器执行完成后，再退出。原因是这里是main goroutine, 如果函数直接返回，那么程序直接就退出了。
			// 当然如果其他监听器长时间执行不完，程序会在signal那里强制杀掉。
			waitForCalled()
		}
	}()

	// 探针标记为就绪状态
	healthManager.MarkReady()
	// 探针加入到全局探针管理器中
	health.AddProbe(healthManager)
	// 启动服务（阻塞），收到信号后，执行我们刚才向ShutdownListener注册的函数，就会触发shutdown http，函数从阻塞中恢复，整个函数退出
	return run(server)
}
