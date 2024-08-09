package sst

import (
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
	buf  []byte
	off  uint64
	size uint64
}

func newBlock() *block {
	return &block{
		buf: make([]byte, BLOCK_SIZE),
	}
}

func initBlock(buf []byte) *block {
	return &block{
		buf: buf,
	}
}

func (b *block) get(key []byte) ([]byte, error) {
	var (
		e       = entry{}
		skey    = common.SearchInternalKey(key)
		off     = 0
		decoded *common.InternalKey
	)
	for {
		n, err := decode(b.buf[off:], &e)
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
		off += n
		if uint64(off) >= BLOCK_SIZE {
			break
		}
	}
	return nil, fmt.Errorf("key not found")
}

func (b *block) add(e *entry) error {
	if e.getSize() > BLOCK_SIZE {
		return errNoSpaceInBlock
	}
	n, err := encode(e, b.buf[b.off:])
	if err != nil {
		return err
	}
	b.off += uint64(n)
	b.size += uint64(n)
	return nil
}

func (b *block) getSize() uint64 {
	return b.size
}
