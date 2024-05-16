package iox

import (
	"bytes"
	"sync"
)

// A BufferPool is a pool to buffer bytes.Buffer objects.
type BufferPool struct {
	// capability 字段在这段代码中的作用是限制池中缓存的 bytes.Buffer 对象的最大容量。
	//具体来说,当调用 Put 方法将一个 bytes.Buffer 对象放回池中时,代码会检查该对象的容量是否小于 capability。如果对象的容量小于 capability，则将其放回池中以供重用;否则,不会将该对象放回池中。
	capability int
	pool       *sync.Pool
}

// NewBufferPool returns a BufferPool.
func NewBufferPool(capability int) *BufferPool {
	return &BufferPool{
		capability: capability,
		pool: &sync.Pool{
			New: func() any {
				return new(bytes.Buffer)
			},
		},
	}
}

// Get returns a bytes.Buffer object from bp.
func (bp *BufferPool) Get() *bytes.Buffer {
	buf := bp.pool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

// Put returns buf into bp.
func (bp *BufferPool) Put(buf *bytes.Buffer) {
	if buf == nil {
		return
	}

	// 如果buf的容量过大，就不放到pool中了。
	if buf.Cap() < bp.capability {
		bp.pool.Put(buf)
	}
}
