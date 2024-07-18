package sst

import (
	"bytes"
	"errors"
	"fmt"
	"godb/log"
	"io"
	"os"
	"strconv"

	"github.com/bits-and-blooms/bloom/v3"
)

func Open(path, sstId string, logger *log.Logger) (*SST, error) {
	f, err := os.OpenFile(fmt.Sprintf("%s.db", path), os.O_RDONLY, F_PERMISSION)
	if err != nil {
		return nil, err
	}

	st, err := os.Stat(fmt.Sprintf("%s.db", path))
	if err != nil {
		return nil, err
	}

	fsize := st.Size()

	buf := make([]byte, 48)
	_, err = f.ReadAt(buf, fsize-48)
	if err != nil {
		return nil, err
	}

	r := bytes.NewReader(buf)
	tm := tableMeta{}

	if err := tm.readFrom(r); err != nil {
		return nil, err
	}

	logger.Debug().
		Uint64("data_offset", tm.dataOffset).
		Uint64("data_size", tm.dataSize).
		Uint64("bloom_filter_offset", tm.bfOffset).
		Uint64("bloom_filter_size", tm.bfSize).
		Uint64("index_offset", tm.indexOffset).
		Uint64("index_size", tm.indexSize).
		Msg("sst metada")

	bfBytes := make([]byte, tm.bfSize)
	_, err = f.ReadAt(bfBytes, int64(tm.bfOffset))
	if err != nil {
		return nil, err
	}

	bf := &bloom.BloomFilter{}
	_, err = bf.ReadFrom(bytes.NewReader(bfBytes))
	if err != nil {
		return nil, err
	}

	idxBlock := make([]byte, tm.indexSize)
	_, err = f.ReadAt(idxBlock, int64(tm.indexOffset))
	if err != nil {
		return nil, err
	}

	return &SST{
		sstId:  sstId,
		meta:   tm,
		bf:     bf,
		idx:    indexFromBuf(bytes.NewBuffer(idxBlock)),
		fref:   f,
		logger: logger,
	}, nil
}

func (s *SST) Contains(k []byte) bool {
	return s.bf.Test(k)
}

func (s *SST) Get(k []byte) ([]byte, error) {
	if !s.bf.Test(k) {
		return nil, ErrNotFoundInBloom
	}
	genCacheKey := func(idx string, off uint64) string {
		return idx + strconv.FormatUint(off, 10)
	}
	getFromBlock := func(raw, key []byte) ([]byte, error) {
		return (&block{buf: bytes.NewBuffer(raw)}).get(key)
	}
	// todo: reformat
	idxEntry, err := s.idx.find(k)
	if err != nil {
		return nil, err
	}
	s.logger.Debug().Str("file", s.fref.Name()).Uint64("offset", idxEntry.foffset).Send()
	if idxEntry.foffset > s.meta.dataSize {
		return nil, fmt.Errorf("index out of bound")
	}
	ck := genCacheKey(s.sstId, idxEntry.foffset)
	if s.blockCache != nil {
		if cEntry, err := s.blockCache.Get(ck); err == nil {
			s.logger.Debug().
				Str("block_entry_id", ck).
				Msg("got block from cache")

			return getFromBlock(cEntry, k)
		}
	}
	rawBlock := make([]byte, BLOCK_SIZE)
	if _, err := s.fref.ReadAt(rawBlock, int64(idxEntry.foffset)); err != nil && !errors.Is(err, io.EOF) {
		s.logger.Error().Err(err).Msg("error while reading block from sst file")
		return nil, err
	}
	if s.blockCache != nil {
		err = s.blockCache.Set(ck, rawBlock)
		if err != nil {
			s.logger.Error().Err(err).Msg("error while caching block")
		}
	}
	return getFromBlock(rawBlock, k)
}
