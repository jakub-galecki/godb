package sst

import (
	"errors"
	"io"
)

type SSTableIter struct {
	sst     *SST
	raw     []byte
	block   *block
	blkIter *BlockIterator
	index   int
}

func (it *SSTableIter) getBlock(i int) (*block, error) {
	if len(it.sst.idx.off) < i {
		return nil, errNoMoreData
	}

	meta := it.sst.idx.off[i]
	cached := func() *block {
		raw := it.sst.getBlockFromCache(meta.foffset)
		if raw != nil {
			return initBlock(raw)
		}
		return nil
	}()
	if cached != nil {
		it.block = cached
		return cached, nil
	}
	clear(it.raw)
	err := it.sst.readRawBlockFromFile(meta.foffset, it.raw)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errNoMoreData
		}
		return nil, err
	}
	b := initBlock(it.raw)
	it.block = b
	return b, nil
}

func NewSSTableIter(sst *SST) (*SSTableIter, error) {
	it := &SSTableIter{
		blkIter: nil,
		sst:     sst,
		raw:     make([]byte, BLOCK_SIZE),
	}

	b, err := it.getBlock(it.index)
	if err != nil {
		return nil, err
	}
	it.blkIter = NewBlockIterator(b)
	it.index++
	return it, nil
}

func (it *SSTableIter) progress() error {
	b, err := it.getBlock(it.index)
	if err != nil {
		return err
	}
	it.blkIter.blk = b
	it.blkIter.off = 0

	err = it.blkIter.Next()
	if err != nil {
		return err
	}
	it.index++
	return nil
}

func (it *SSTableIter) Next() error {
	err := it.blkIter.Next()
	if err != nil {
		if !errors.Is(err, errNoMoreData) {
			return err
		}
		if err := it.progress(); err != nil {
			return err
		}
	}

}

func (it *SSTableIter) Key() []byte {
	return it.blkIter.Key()
}

func (it *SSTableIter) Value() []byte {
	return it.blkIter.Value()
}
