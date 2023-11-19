package sst

import (
	"bytes"
	"fmt"
)

const (
	// Maximum block size. When it reaches this size it will be flushed to disk
	BLOCK_SIZE = 1 << 12

	F_PREFIX = "data_block.bin"
)

type block struct {
	min []byte
	// max []byte

	buf *bytes.Buffer

	size int
}

func newBlock() *block {
	return &block{
		buf: new(bytes.Buffer),
	}
}

func (b *block) get(key []byte) ([]byte, error) {
	// todo: binary search
	//e := entry{}

	//for err := decode(b.buf, &e); err == nil; {
	//	if bytes.Compare(e.key, key) {
	//		return e.value, nil
	//	}
	//}
	//
	return nil, fmt.Errorf("key not found")
}

func (b *block) getMin(key []byte) []byte {
	var (
		min []byte
	)

	if len(b.min) == 0 {
		return key
	}

	switch c := bytes.Compare(key, b.min); {
	case c > 0, c == 0:
		min = b.min
	case c < 0:
		min = key
	}

	return min
}

func (b *block) add(e *entry) error {
	b.min = b.getMin(e.key)
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
