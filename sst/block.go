package sst

import (
	"bytes"
	"errors"
	"fmt"
	"godb/common"
	"io"
)

const (
	// Maximum block size. When it reaches this size it will be flushed to disk
	BLOCK_SIZE uint64 = 4096

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
	var (
		e       = entry{}
		skey    = common.SearchInternalKey(key)
		read    = 0
		decoded *common.InternalKey
	)
	for {
		n, err := decode(b.buf, &e)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		decoded = common.DeserializeKey(e.key)
		if decoded == nil {
			return nil, common.ErrKeyNotFound
		}
		if skey.SoftEqual(decoded) {
			return e.value, nil
		}
		read += n
		if uint64(read) >= BLOCK_SIZE {
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
