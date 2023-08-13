package sst

import (
	"errors"
)

type ReaderOpts struct {
	dirPath string
}

func (s *sst) Contains(k []byte) bool {
	return s.bf.MayContain(k)
}

func (s *sst) Get(k []byte) ([]byte, error) {
	if !s.Contains(k) {
		return nil, errors.New("key not found")
	}

	idx := s.index.Get(k)
	if idx > s.blocks.getSize() {
		return nil, errors.New("index out of bound")
	}

	// todo: add block caching
	block := s.blocks.getAt(idx)

	return block.get(k)
}
