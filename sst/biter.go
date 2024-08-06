package sst

import (
	"errors"
	"io"
)

var errNoMoreData = errors.New("block has no more data")

type BlockIterator struct {
	blk  *block
	cure *entry
}

func NewBlockIterator(blk *block) *BlockIterator {
	return &BlockIterator{
		blk:  blk,
		cure: new(entry),
	}
}

func (b *BlockIterator) Next() (bool, error) {
	_, err := decode(b.blk.buf, b.cure)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return false, errNoMoreData
		}
		return false, err
	}
	return true, nil
}

func (b *BlockIterator) Key() []byte {
	return b.cure.key
}

func (b *BlockIterator) Value() []byte {
	return b.cure.value
}
