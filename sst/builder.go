package sst

import (
	"bytes"
	"os"
)

type Builder interface {
	WriteNext([]byte, []byte) (Builder, error)
	Finish() Block
}

func NewBuilder(db string) Builder {
	return &builder{
		db: db,
	}
}

type builder struct {
	bId          int
	fullBlocks   []*block
	currentBlock *block
	currentFile  *os.File
	db           string
}

func (b *builder) flush() error {
	b.currentBlock.clearBuf()
	return nil
}

func (b *builder) WriteNext(key, value []byte) (Builder, error) {
	if len(key)+len(value) > BLOCK_SIZE {
		if err := b.flush(); err != nil {
			return nil, err
		}
		b.fullBlocks = append(b.fullBlocks, b.currentBlock)
		b.currentBlock = newBlock()
	}

	switch bytes.Compare(b.currentBlock.min, key) {
	case 1:
		b.currentBlock.min = key
	default:
	}

	switch bytes.Compare(b.currentBlock.max, key) {
	case 1:
		b.currentBlock.max = key
	default:
	}

	return b, nil
}

func (b *builder) Finish() Block {
	return nil
}
