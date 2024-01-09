package handler

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/rest/httpx"
	"github.com/zeromicro/go-zero/rest/internal"
)

const (
	statusClientClosedRequest = 499
	reason                    = "Request Timeout"
	headerUpgrade             = "Upgrade"
	valueWebsocket            = "websocket"
)

// TimeoutHandler returns the handler with given timeout.
// If client closed request, code 499 will be logged.
// Notice: even if canceled in server side, 499 will be logged as well.
func TimeoutHandler(duration time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if duration <= 0 {
			// 不做包装，直接返回原函数
			return next
		}
		// 处理请求超时时，直接给客户端返回。
		return &timeoutHandler{
			handler: next,
			dt:      duration,
		}
	}
}

// timeoutHandler is the handler that controls the request timeout.
// Why we implement it on our own, because the stdlib implementation
// treats the ClientClosedRequest as http.StatusServiceUnavailable.
// And we write the codes in logs as code 499, which is defined by nginx.
type timeoutHandler struct {
	handler http.Handler
	dt      time.Duration
}

func (h *timeoutHandler) errorBody() string {
	return reason
}

func (h *timeoutHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get(headerUpgrade) == valueWebsocket {
		h.handler.ServeHTTP(w, r)
		return
	}

	ctx, cancelCtx := context.WithTimeout(r.Context(), h.dt)
	defer cancelCtx()

	// 使用 timeout context 覆盖原来的context
	r = r.WithContext(ctx)
	// 业务正常处理完成信号
	done := make(chan struct{})
	tw := &timeoutWriter{
		w:    w,
		h:    make(http.Header),
		req:  r,
		code: http.StatusOK,
	}
	panicChan := make(chan any, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				panicChan <- p
			}
		}()
		h.handler.ServeHTTP(tw, r)
		close(done)
	}()
	select {
	case p := <-panicChan:
		panic(p)
	case <-done:
		// 业务正常处理完成（未超时）
		tw.mu.Lock()
		defer tw.mu.Unlock()
		dst := w.Header()
		for k, vv := range tw.h {
			dst[k] = vv
		}

		// We don't need to write header 200, because it's written by default.
		// If we write it again, it will cause a warning: `http: superfluous response.WriteHeader call`.
		if tw.code != http.StatusOK {
			w.WriteHeader(tw.code)
		}
		w.Write(tw.wbuf.Bytes())
	case <-ctx.Done():
		// 业务处理超时
		tw.mu.Lock()
		defer tw.mu.Unlock()
		// there isn't any user-defined middleware before TimoutHandler,
		// so we can guarantee that cancelation in biz related code won't come here.
		httpx.ErrorCtx(r.Context(), w, ctx.Err(), func(w http.ResponseWriter, err error) {
			if errors.Is(err, context.Canceled) {
				// 499状态码来表示客户端在服务器响应之前关闭了连接，这通常发生在客户端发起请求后，但在服务器完成响应之前中断连接。
				// HTTP状态码499通常指示客户端关闭连接，而服务器在尚未完成其响应时收到了连接关闭的请求。这个状态码在HTTP标准中并没有明确的定义，它更多地是由一些Web服务器或代理服务器特定地使用。
				w.WriteHeader(statusClientClosedRequest)
			} else {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			io.WriteString(w, h.errorBody())
		})
		tw.timedOut = true
	}
}

type timeoutWriter struct {
	w    http.ResponseWriter
	h    http.Header
	wbuf bytes.Buffer
	req  *http.Request

	mu          sync.Mutex
	timedOut    bool
	wroteHeader bool
	code        int
}

var _ http.Pusher = (*timeoutWriter)(nil)

// Flush implements the Flusher interface.
func (tw *timeoutWriter) Flush() {
	flusher, ok := tw.w.(http.Flusher)
	if !ok {
		return
	}

	header := tw.w.Header()
	for k, v := range tw.h {
		header[k] = v
	}

	tw.w.Write(tw.wbuf.Bytes())
	tw.wbuf.Reset()
	flusher.Flush()
}

// Header returns the underline temporary http.Header.
func (tw *timeoutWriter) Header() http.Header {
	return tw.h
}

// Hijack implements the Hijacker interface.
func (tw *timeoutWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hijacked, ok := tw.w.(http.Hijacker); ok {
		return hijacked.Hijack()
	}

	return nil, nil, errors.New("server doesn't support hijacking")
}

// Push implements the Pusher interface.
func (tw *timeoutWriter) Push(target string, opts *http.PushOptions) error {
	if pusher, ok := tw.w.(http.Pusher); ok {
		return pusher.Push(target, opts)
	}

	return http.ErrNotSupported
}

// Write writes the data to the connection as part of an HTTP reply.
// Timeout and multiple header written are guarded.
func (tw *timeoutWriter) Write(p []byte) (int, error) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.timedOut {
		return 0, http.ErrHandlerTimeout
	}

	if !tw.wroteHeader {
		tw.writeHeaderLocked(http.StatusOK)
	}

	return tw.wbuf.Write(p)
}

func (tw *timeoutWriter) writeHeaderLocked(code int) {
	checkWriteHeaderCode(code)

	switch {
	case tw.timedOut:
		return
	case tw.wroteHeader:
		if tw.req != nil {
			caller := relevantCaller()
			internal.Errorf(tw.req, "http: superfluous response.WriteHeader call from %s (%s:%d)",
				caller.Function, path.Base(caller.File), caller.Line)
		}
	default:
		tw.wroteHeader = true
		tw.code = code
	}
}

func (tw *timeoutWriter) WriteHeader(code int) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if !tw.wroteHeader {
		tw.writeHeaderLocked(code)
	}
}

func checkWriteHeaderCode(code int) {
	if code < 100 || code > 599 {
		panic(fmt.Sprintf("invalid WriteHeader code %v", code))
	}
}

// relevantCaller searches the call stack for the first function outside of net/http.
// The purpose of this function is to provide more helpful error messages.
func relevantCaller() runtime.Frame {
	pc := make([]uintptr, 16)
	n := runtime.Callers(1, pc)
	frames := runtime.CallersFrames(pc[:n])
	var frame runtime.Frame
	for {
		frame, more := frames.Next()
		if !strings.HasPrefix(frame.Function, "net/http.") {
			return frame
		}

		if !more {
			break
		}
	}

	return frame
}
