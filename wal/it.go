package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

type Iterator struct {
	reader *bufio.Reader
	b      *block
}

func NewIterator(f io.Reader) (*Iterator, error) {
	r := bufio.NewReader(f)
	it := &Iterator{
		reader: r,
		b:      &block{},
	}
	return it, it.loadBlock()
}

func (it *Iterator) Next() ([]byte, error) {
	if it.b.off >= it.b.size {
		return nil, io.EOF
	}
	dataLen, read := binary.Uvarint(it.b.buf[it.b.off:])
	if dataLen == 0 {
		err := it.loadBlock()
		if err != nil {
			return nil, err
		}
		// try to read from the new block
		dataLen, read = binary.Uvarint(it.b.buf[it.b.off:])
	}
	end := it.b.off + int(dataLen) + read
	buf := it.b.buf[it.b.off:end]
	it.b.off += end
	data := make([]byte, dataLen)
	copy(data, buf[read:])
	return data, nil
}

func Iter(it *Iterator, f func(raw []byte) error) error {
	for {
		wr, err := it.Next()
		if errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return err
		}

		if err := f(wr); err != nil {
			return err
		}
	}
}

func (it *Iterator) loadBlock() error {
	var err error
	it.b.size, err = io.ReadFull(it.reader, it.b.buf[:])
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return err
	}
	it.b.off = 0
	return nil
}
