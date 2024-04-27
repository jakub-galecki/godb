package wal

import (
	"io"
	"os"
)

type Iterator struct {
	reader io.Reader
}

func NewIterator(f *os.File) (*Iterator, error) {
	return &Iterator{
		reader: f,
	}, nil
}

func (it *Iterator) Next() (string, []byte, []byte) {
	return "", nil, nil
}
