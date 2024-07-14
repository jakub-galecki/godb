package sst

import (
	"bytes"
	"fmt"
)

const (
	// Maximum block size. When it reaches this size it will be flushed to disk
	BLOCK_SIZE = 1 << 10

	F_PREFIX = "data_block.bin"
)

type block struct {
	buf  *bytes.Buffer
	size uint64
}

func newBlock() *block {
	return &block{
		buf: new(bytes.Buffer),
	}
}

func (b *block) get(key []byte) ([]byte, error) {
	// maybe: implement block offests and binary search
	e := entry{}
	read := 0
	for n, err := decode(b.buf, &e); err == nil; n, err = decode(b.buf, &e) {
		if bytes.Equal(e.key, key) {
			return e.value, nil
		}
		read += n
		if read >= BLOCK_SIZE {
			break
		}
	}
	return nil, fmt.Errorf("key not found")
}

func (b *block) add(e *entry) error {
	n, err := encode(e, b.buf)
	if err != nil {
		return err
	}
	b.size += uint64(n)
	return nil
}

func (b *block) getSize() uint64 {
	return b.size
}
