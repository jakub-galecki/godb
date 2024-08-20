package sst

import (
	"errors"
	"github.com/jakub-galecki/godb/common"
)

var errNoSpaceInBlock = errors.New("no space in current block")

type blockBuilder struct {
	cur      *block
	min, max []byte
}

func newBlockBuilder() *blockBuilder {
	bb := &blockBuilder{
		cur: newBlock(),
	}
	return bb
}

func (b *blockBuilder) updateMinMax(key []byte) {
	if len(b.min) == 0 && len(b.max) == 0 {
		b.min = key
		b.max = key
		return
	}
	b.min = common.Min(b.min, key)
	b.max = common.Max(b.max, key)
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
	b.updateMinMax(e.rawKey.UserKey)
	return nil
}

func (b *blockBuilder) hasSpace(additionalSize uint64) bool {
	return b.cur.getSize()+additionalSize <= BLOCK_SIZE
}

func (b *blockBuilder) finish() (*block, []byte, []byte) {
	return b.cur, b.min, b.max
}

func (b *blockBuilder) rotateBlock() (*block, []byte, []byte) {
	res, min, max := b.finish()
	b.cur = newBlock()
	b.min = nil
	b.max = nil
	return res, min, max
}
