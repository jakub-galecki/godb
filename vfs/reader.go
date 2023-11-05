package vfs

import (
	"errors"
	"io"
)

func (v vfs[T]) Read(data []byte) (int, error) {
	n, err := v.f.Read(data)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	return n, nil
}
