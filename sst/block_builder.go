package sst

import (
	"bytes"
	"errors"
)

var errNoSpaceInBlock = errors.New("no space in current block")

type blockBuilder struct {
	cur *block
	min []byte
}

func newBlockBuilder() *blockBuilder {
	bb := &blockBuilder{
		cur: newBlock(),
	}
	return bb
}

func (b *blockBuilder) updateMin(key []byte) {
	var (
		min []byte
	)

	if len(b.min) == 0 {
		b.min = key
		return
	}

	switch c := bytes.Compare(key, b.min); {
	case c > 0, c == 0:
		min = b.min
	default:
		min = key
	}

	b.min = min
}

func (b *blockBuilder) add(e *entry) error {
	// ensure that written block size will not be greater than BLOCK_SIZE
	if !b.hasSpace(e.getSize()) {
		return errNoSpaceInBlock
	}
	err := b.cur.add(e)
	if err != nil {
		return err
	}
	b.updateMin(e.rawKey.UserKey)
	return nil
}

func (b *blockBuilder) hasSpace(additionalSize uint64) bool {
	return b.cur.getSize()+additionalSize <= BLOCK_SIZE
}

func (b *blockBuilder) finish() (*block, []byte) {
	return b.cur, b.min
}

func (b *blockBuilder) rotateBlock() (*block, []byte) {
	res, min := b.finish()
	b.cur = newBlock()
	b.min = nil
	return res, min
}
