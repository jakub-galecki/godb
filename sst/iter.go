package sst

import (
	"errors"
	"github.com/jakub-galecki/godb/common"
	"io"
)

var _ common.Iterator = (*SSTableIter)(nil)

type SSTableIter struct {
	sst     *SST
	raw     []byte
	block   *block
	blkIter *BlockIterator
	index   int
}

func (it *SSTableIter) getBlock(i int) (*block, error) {
	if len(it.sst.idx.off) <= i {
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
		blkIter: NewBlockIterator(nil),
		sst:     sst,
		raw:     make([]byte, BLOCK_SIZE),
	}
	return it, nil
}

func (it *SSTableIter) progressToNextBlock() error {
	b, err := it.getBlock(it.index)
	if err != nil {
		return err
	}
	it.blkIter.resetWithNewBlock(b)
	it.index++
	return nil
}

func (it *SSTableIter) Next() (*common.InternalKey, []byte, error) {
	key, value, err := it.blkIter.Next()
	if err != nil {
		if !errors.Is(err, errNoMoreData) {
			return nil, nil, err
		}
		if err := it.progressToNextBlock(); err != nil {
			return nil, nil, common.ErrIteratorExhausted
		}
		key, value, err = it.blkIter.Next()
		if err != nil {
			return nil, nil, common.ErrIteratorExhausted
		}
	}
	return key, value, nil
}

func (it *SSTableIter) Key() *common.InternalKey {
	if it.blkIter == nil {
		return nil
	}
	return it.blkIter.Key()
}

func (it *SSTableIter) Value() []byte {
	if it.blkIter == nil {
		return nil
	}
	return it.blkIter.Value()
}

func (it *SSTableIter) SeekToFirst() (*common.InternalKey, []byte, error) {
	it.index = 0
	if err := it.progressToNextBlock(); err != nil {
		return nil, nil, err
	}
	return it.blkIter.SeekToFirst()
}

func (it *SSTableIter) Valid() bool {
	return it.blkIter.Valid()
}
