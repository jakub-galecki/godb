package sst

import (
	"errors"
	"godb/common"
)

var errNoMoreData = errors.New("block has no more data")

type BlockIterator struct {
	blk  *block
	cure *entry
	off  int
}

func NewBlockIterator(blk *block) *BlockIterator {
	return &BlockIterator{
		blk:  blk,
		cure: new(entry),
	}
}

func (b *BlockIterator) Next() error {
	if uint64(b.off) >= BLOCK_SIZE {
		return errNoMoreData
	}
	n, err := decode(b.blk.buf[b.off:], b.cure)
	if err != nil {
		return err
	}
	b.cure.rawKey = common.DeserializeKey(b.cure.key)
	b.off += n
	return nil
}

func (b *BlockIterator) Key() []byte {
	if b.cure.rawKey == nil {
		return nil
	}
	return b.cure.rawKey.UserKey
}

func (b *BlockIterator) Value() []byte {
	if b.cure == nil {
		return nil
	}
	return b.cure.value
}

func (b *BlockIterator) Valid() bool {
	return len(b.Key()) > 0 // value maybe nil for tombstone
}
