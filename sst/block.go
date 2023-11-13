package sst

import (
	"bytes"
)

const (
	// Maximum block size. When it reaches this size it will be flushed to disk
	BLOCK_SIZE = 1 << 12

	F_PREFIX = "data_block.bin"
)

type block struct {
	min []byte
	max []byte

	buf *bytes.Buffer

	size int
}

func newBlock() *block {
	return &block{
		buf: new(bytes.Buffer),
	}
}

func (b *block) get(key []byte) ([]byte, error) {
	return nil, nil
}

func (b *block) getMinMax(key []byte) ([]byte, []byte) {
	var (
		min, max []byte
	)
	switch c := bytes.Compare(key, b.min); {
	case c > 0, c == 0:
		min = b.min
	case c < 0:
		min = key
	}

	switch c := bytes.Compare(key, b.max); {
	case c < 0, c == 0:
		max = b.max
	case c > 0:
		max = key
	}
	return min, max
}

func (b *block) add(e *entry) error {
	b.min, b.max = b.getMinMax(e.key)
	n, err := encode(e, b.buf)
	if err != nil {
		return err
	}
	b.size += n
	return nil
}

func (b *block) getSize() int {
	return b.size
}

// type blockGroup struct {
// 	ready []*block
// 	size  int
// 	// we should also store the information whether this is the first or nth block group
// 	// for memetable
// }

// func newBlockGroup() *blockGroup {
// 	return &blockGroup{
// 		ready: make([]*block, 0),
// 		size:  0,
// 	}
// }

// func (bg *blockGroup) add(b *block) {
// 	bg.ready = append(bg.ready, b)
// 	bg.size++
// }

// func (bg *blockGroup) get(key []byte) (*block, error) {
// 	// quick search
// 	return nil, nil
// }

// func (bg *blockGroup) getSize() int {
// 	return bg.size
// }

// func (bg *blockGroup) getAt(i int) *block {
// 	return bg.ready[i]
// }

// func (bg *blockGroup) iter() blockIterator {
// 	return nil
// }

// type blocks interface {
// 	get([]byte) (*block, error)
// 	add(*block)
// 	iter() blockIterator
// 	getSize() int
// 	getAt(int) *block
// }

// type blockIterator interface {
// }
