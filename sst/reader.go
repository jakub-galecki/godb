package sst

import (
	"bytes"
	"errors"
	"fmt"
	"godb/common"
	"godb/log"
	"io"
	"os"
	"path"
	"strconv"

	"github.com/bits-and-blooms/bloom/v3"
)

func Open(dir, sstId string, logger *log.Logger) (*SST, error) {
	sstPath := path.Join(dir, sstId+".db")
	f, err := os.OpenFile(sstPath, os.O_RDONLY, F_PERMISSION)
	if err != nil {
		return nil, err
	}

	st, err := os.Stat(sstPath)
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
		idx:    indexFromBuf(idxBlock),
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
	getFromBlock := func(raw, key []byte) ([]byte, error) {
		return (&block{buf: raw}).get(key)
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
	b := s.getBlockFromCache(idxEntry.foffset)
	if b != nil {
		return getFromBlock(b, k)
	}
	rawBlock := make([]byte, BLOCK_SIZE)
	err = s.readRawBlockFromFile(idxEntry.foffset, rawBlock)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, common.ErrKeyNotFound
		}
		return nil, err
	}
	s.setBlockInCache(idxEntry.foffset, rawBlock)
	return getFromBlock(rawBlock, k)
}

func (s *SST) getBlockFromCache(blockOff uint64) []byte {
	if s.blockCache == nil {
		return nil
	}
	ck := s.getCacheKey(blockOff)
	if cEntry, err := s.blockCache.Get(ck); err == nil {
		s.logger.Debug().
			Str("block_entry_id", ck).
			Msg("got block from cache")
		return cEntry
	}
	return nil
}

func (s *SST) setBlockInCache(blockOff uint64, b []byte) {
	if s.blockCache == nil {
		return
	}
	ck := s.getCacheKey(blockOff)
	if s.blockCache != nil {
		err := s.blockCache.Set(ck, b)
		if err != nil {
			s.logger.Error().Err(err).Msg("error while caching block")
		}
	}
}

func (s *SST) getCacheKey(blockOff uint64) string {
	return s.sstId + strconv.FormatUint(blockOff, 10)
}

func (s *SST) readRawBlockFromFile(off uint64, rawBlock []byte) error {
	_, err := s.fref.ReadAt(rawBlock, int64(off))
	if err != nil {
		s.logger.Error().Err(err).Msg("error while reading block from sst file")
		return err
	}
	return nil
}
