package iox

import (
	"bufio"
	"io"
	"strings"
)

// 根据给定reader，每次读取一行。 它这个封装主要是实现了一个调用方法
/*
	while s.Scan() {
		line := s.Line()
	}
*/

// A TextLineScanner is a scanner that can scan lines from given reader.
type TextLineScanner struct {
	reader  *bufio.Reader
	hasNext bool
	line    string
	err     error
}

// NewTextLineScanner returns a TextLineScanner with given reader.
func NewTextLineScanner(reader io.Reader) *TextLineScanner {
	return &TextLineScanner{
		reader:  bufio.NewReader(reader),
		hasNext: true,
	}
}

// Scan checks if scanner has more lines to read.
func (scanner *TextLineScanner) Scan() bool {
	if !scanner.hasNext {
		return false
	}

	// 读取一行： ReadString函数，是一直读，直到读到给定符号为止。
	line, err := scanner.reader.ReadString('\n')
	scanner.line = strings.TrimRight(line, "\n")
	if err == io.EOF {
		scanner.hasNext = false
		return true
	} else if err != nil {
		scanner.err = err
		return false
	}
	return true
}

// Line returns the next available line.
func (scanner *TextLineScanner) Line() (string, error) {
	return scanner.line, scanner.err
}
