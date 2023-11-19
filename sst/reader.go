package sst

import (
	"bytes"
	"fmt"
	"os"

	"github.com/bits-and-blooms/bloom"
)

type ReaderOpts struct {
	dirPath string
}

func Open(table string) SST {
	dbname := fmt.Sprintf("%s.db", table)
	f, err := os.OpenFile(dbname, os.O_RDONLY, F_PERMISSION)
	if err != nil {
		panic(err)
	}

	st, err := os.Stat(dbname)
	if err != nil {
		panic(err)
	}

	fsize := st.Size()

	buf := make([]byte, 48)
	_, err = f.ReadAt(buf, fsize-48)
	if err != nil {
		panic(err)
	}

	r := bytes.NewReader(buf)
	tm := tableMeta{}

	if err := tm.readFrom(r); err != nil {
		panic(err)
	}

	logger.Debugf("dataOffset: %d, dataSize: %d, bfOffset: %d, bfSize: %d, indexOffset: %d, indexSize: %d",
		tm.dataOffset, tm.dataSize, tm.bfOffset, tm.bfSize, tm.indexOffset, tm.indexSize)

	bfBytes := make([]byte, tm.bfSize)
	_, err = f.ReadAt(bfBytes, int64(tm.bfOffset))
	if err != nil {
		panic(err)
	}

	bf := &bloom.BloomFilter{}
	_, err = bf.ReadFrom(bytes.NewReader(bfBytes))
	if err != nil {
		panic(err)
	}

	idxBlock := make([]byte, tm.indexSize)
	_, err = f.ReadAt(idxBlock, int64(tm.indexOffset))
	if err != nil {
		panic(err)
	}

	return &sst{
		meta: tm,
		bf:   bf,
		idx:  indexFromBuf(bytes.NewBuffer(idxBlock)),
	}
}

func (s *sst) Contains(k []byte) bool {
	return s.bf.Test(k)
}

func (s *sst) Get(k []byte) ([]byte, error) {
	if !s.bf.Test(k) {
		return nil, fmt.Errorf("not found in bloom")
	}

	// todo: format
	idxEntry, err := s.idx.find(k)
	if err != nil {
		return nil, err
	}

	logger.Debugf("offset %d", idxEntry.foffset)
	if idxEntry.foffset > s.meta.dataSize {
		return nil, fmt.Errorf("index out of bound")
	}

	// // todo: add block caching
	// block := s.blocks.getAt(idx)

	// return block.get(k)
	return nil, nil
}
