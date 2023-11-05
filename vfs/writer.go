package vfs

func (v vfs[T]) Write(data []byte) (n int, err error) {
	return v.f.Write(data)
}
