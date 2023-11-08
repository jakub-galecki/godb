package vfs

func (v vfs[T]) Write(data []byte) (n int, err error) {
	return v.f.Write(data)
}

func (v vfs[T]) Flush() error {
	return v.f.Sync()
}
