package sst

import (
	"errors"
	"godb/common"
)

var errNoMoreData = errors.New("block has no more data")

var _ common.Iterator = (*BlockIterator)(nil)

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

func (b *BlockIterator) Next() (*common.InternalKey, []byte, error) {
	if uint64(b.off) >= BLOCK_SIZE {
		return nil, nil, errNoMoreData
	}
	n, err := decode(b.blk.buf[b.off:], b.cure)
	if err != nil {
		return nil, nil, err
	}
	b.cure.rawKey = common.DeserializeKey(b.cure.key)
	b.off += n
	return b.cure.rawKey, b.cure.value, nil
}

func (b *BlockIterator) Valid() bool {
	if b.cure == nil || b.cure.rawKey == nil {
		return false
	}
	return len(b.cure.rawKey.UserKey) > 0 // value maybe nil for tombstone
}

func (b *BlockIterator) Key() *common.InternalKey {
	return b.cure.rawKey
}

func (b *BlockIterator) Value() []byte {
	return b.cure.value
}
