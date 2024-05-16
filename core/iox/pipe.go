package iox

import "os"

// 将标准输入，和标准输入，重定向到一个管道（Pipe）的读端和写端。并支持在后续将 标准输入，和标准输入重新指向os.Stdin 和 os.Stdout

// RedirectInOut redirects stdin to r, stdout to w, and callers need to call restore afterwards.
func RedirectInOut() (restore func(), err error) {
	var r, w *os.File
	r, w, err = os.Pipe()
	if err != nil {
		return
	}

	ow := os.Stdout
	os.Stdout = w
	or := os.Stdin
	os.Stdin = r
	restore = func() {
		os.Stdin = or
		os.Stdout = ow
	}

	return
}
