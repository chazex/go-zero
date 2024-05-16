package iox

import "io"

// 一个不需要关闭的Write？

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error {
	return nil
}

// NopCloser returns an io.WriteCloser that does nothing on calling Close.
func NopCloser(w io.Writer) io.WriteCloser {
	return nopCloser{w}
}
