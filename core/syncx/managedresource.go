package syncx

import "sync"

// 管理单个资源，资源可以被删除。资源不存在的时候，可以生成。

// A ManagedResource is used to manage a resource that might be broken and refetched, like a connection.
type ManagedResource struct {
	resource any
	lock     sync.RWMutex
	// 生成资源的函数
	generate func() any
	//判断资源是否相等的函数
	equals func(a, b any) bool
}

// NewManagedResource returns a ManagedResource.
func NewManagedResource(generate func() any, equals func(a, b any) bool) *ManagedResource {
	return &ManagedResource{
		generate: generate,
		equals:   equals,
	}
}

// MarkBroken marks the resource broken.
func (mr *ManagedResource) MarkBroken(resource any) {
	mr.lock.Lock()
	defer mr.lock.Unlock()

	// 优先判断资源是否相等，相等的话才会清除资源。
	if mr.equals(mr.resource, resource) {
		mr.resource = nil
	}
}

// Take takes the resource, if not loaded, generates it.
func (mr *ManagedResource) Take() any {
	mr.lock.RLock()
	resource := mr.resource
	mr.lock.RUnlock()

	if resource != nil {
		// 资源存在直接返回
		return resource
	}

	mr.lock.Lock()
	defer mr.lock.Unlock()
	// maybe another Take() call already generated the resource.
	if mr.resource == nil {
		mr.resource = mr.generate()
	}
	return mr.resource
}
