package sst

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/bits-and-blooms/bloom"
)

type ReaderOpts struct {
	dirPath string
}

func Open(table string) *SST {
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

	return &SST{
		meta: tm,
		bf:   bf,
		idx:  indexFromBuf(bytes.NewBuffer(idxBlock)),
		fref: f,
	}
}

func (s *SST) Contains(k []byte) bool {
	return s.bf.Test(k)
}

func (s *SST) Get(k []byte) ([]byte, error) {
	if !s.bf.Test(k) {
		return nil, fmt.Errorf("not found in bloom")
	}

	genCacheKey := func(idx int, off uint64) string {
		return fmt.Sprintf("%d.%d", idx, off)
	}

	getFromBlock := func(raw, key []byte) ([]byte, error) {
		return (&block{buf: bytes.NewBuffer(raw)}).get(k)
	}

	// todo: reformat
	idxEntry, err := s.idx.find(k)
	if err != nil {
		return nil, err
	}

	// logger.Debugf("offset %d", idxEntry.foffset)
	if idxEntry.foffset > s.meta.dataSize {
		return nil, fmt.Errorf("index out of bound")
	}

	ck := genCacheKey(s.sstId, idxEntry.foffset)

	if s.blockCache != nil {
		if cEntry, err := s.blockCache.Get(ck); err == nil {
			return getFromBlock(cEntry, k)
		}
	}

	rawBlock := make([]byte, BLOCK_SIZE)
	if _, err := s.fref.ReadAt(rawBlock, int64(idxEntry.foffset)); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	if s.blockCache != nil {
		err = s.blockCache.Set(ck, rawBlock)
	}

	return getFromBlock(rawBlock, k)
}
