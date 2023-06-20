package vfs

import (
	"errors"
	"io"
)

func (v vfs[T]) Read(obj *T, decode func([]byte) (T, error)) (int, error) {
	var (
		buf []byte
	)

	n, err := v.f.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}

	res, err := decode(buf)
	if err != nil {
		return 0, err
	}

	*obj = res
	return n, nil
}
