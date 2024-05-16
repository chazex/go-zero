package iox

import (
	"bytes"
	"errors"
	"io"
	"os"
)

const bufSize = 32 * 1024

// 统计一个文件有多少行。
// 它这个统计方法和我的思路不一样： 它首先是申请了一块内存，然后每次都把这块内存读满， 然后统计这块内存中有多少个'\n'。
// 我可能就会每次读取一行，看看最终读多少行，很呆。

// CountLines returns the number of lines in file.
func CountLines(file string) (int, error) {
	f, err := os.Open(file)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var noEol bool
	buf := make([]byte, bufSize)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := f.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case errors.Is(err, io.EOF):
			if noEol {
				count++
			}
			return count, nil
		case err != nil:
			return count, err
		}

		noEol = c > 0 && buf[c-1] != '\n'
	}
}
