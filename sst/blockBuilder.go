package sst

import (
	"bytes"
	"godb/common"
)

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

func (b *blockBuilder) add(key *common.InternalKey, value []byte) error {
	err := b.cur.add(&entry{key: key.Serialize(), value: value})
	if err != nil {
		return err
	}
	b.updateMin(key.UserKey)
	return nil
}

func (b *blockBuilder) hasSpace(additionalSize uint64) bool {
	return b.cur.getSize()+additionalSize > BLOCK_SIZE
}

func (b *blockBuilder) finish() (*block, []byte) {
	return b.cur, b.min
}
