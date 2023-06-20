package vfs

func (v vfs[T]) Write(obj T, encode func(T) ([]byte, error)) (n int, err error) {

	buf, err := encode(obj)
	if err != nil {
		return 0, err
	}

	return v.f.Write(buf)
}
