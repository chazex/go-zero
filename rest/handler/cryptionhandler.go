package handler

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"

	"github.com/zeromicro/go-zero/core/codec"
	"github.com/zeromicro/go-zero/core/logx"
)

const maxBytes = 1 << 20 // 1 MiB

var errContentLengthExceeded = errors.New("content length exceeded")

// CryptionHandler returns a middleware to handle cryption.
func CryptionHandler(key []byte) func(http.Handler) http.Handler {
	return LimitCryptionHandler(maxBytes, key)
}

// LimitCryptionHandler returns a middleware to handle cryption.
func LimitCryptionHandler(limitBytes int64, key []byte) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cw := newCryptionResponseWriter(w)
			defer cw.flush(key)

			if r.ContentLength <= 0 {
				next.ServeHTTP(cw, r)
				return
			}

			// 对Body解密
			if err := decryptBody(limitBytes, key, r); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			next.ServeHTTP(cw, r)
		})
	}
}

func decryptBody(limitBytes int64, key []byte, r *http.Request) error {
	// 内容过长， 则直接返回错误。 （可能是因为内容长，解密比较费时、费资源吧）
	if limitBytes > 0 && r.ContentLength > limitBytes {
		return errContentLengthExceeded
	}

	var content []byte
	var err error
	if r.ContentLength > 0 {
		// 可以从头中，获取到Content-Length, 则读取全部消息体到content中
		content = make([]byte, r.ContentLength)
		_, err = io.ReadFull(r.Body, content)
	} else {
		// 如果没有Content-Length， 我们不确定消息体到底多大，不能一下都读进来，容易爆炸。 所以此时无论多长，我们最多只读1M
		// 有的客户端就是不设置Content-Length, 我们不能把这种请求拒掉。 如果它的长度超过1M， 此时我们就只读取了一部分，后面解密的时候肯定会失败。
		content, err = io.ReadAll(io.LimitReader(r.Body, maxBytes))
	}
	if err != nil {
		return err
	}

	content, err = base64.StdEncoding.DecodeString(string(content))
	if err != nil {
		return err
	}

	// 对body解密
	output, err := codec.EcbDecrypt(key, content)
	if err != nil {
		// 解密失败
		return err
	}

	// 我们把解密的消息体重新放回Body中。
	var buf bytes.Buffer
	buf.Write(output)
	r.Body = io.NopCloser(&buf)

	return nil
}

type cryptionResponseWriter struct {
	http.ResponseWriter
	buf *bytes.Buffer
}

func newCryptionResponseWriter(w http.ResponseWriter) *cryptionResponseWriter {
	return &cryptionResponseWriter{
		ResponseWriter: w,
		buf:            new(bytes.Buffer),
	}
}

func (w *cryptionResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *cryptionResponseWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

// Hijack implements the http.Hijacker interface.
// This expands the Response to fulfill http.Hijacker if the underlying http.ResponseWriter supports it.
func (w *cryptionResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hijacked, ok := w.ResponseWriter.(http.Hijacker); ok {
		return hijacked.Hijack()
	}

	return nil, nil, errors.New("server doesn't support hijacking")
}

func (w *cryptionResponseWriter) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *cryptionResponseWriter) WriteHeader(statusCode int) {
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *cryptionResponseWriter) flush(key []byte) {
	if w.buf.Len() == 0 {
		return
	}

	content, err := codec.EcbEncrypt(key, w.buf.Bytes())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	body := base64.StdEncoding.EncodeToString(content)
	if n, err := io.WriteString(w.ResponseWriter, body); err != nil {
		logx.Errorf("write response failed, error: %s", err)
	} else if n < len(body) {
		logx.Errorf("actual bytes: %d, written bytes: %d", len(body), n)
	}
}
