package sst

import (
	"os"

	"github.com/bits-and-blooms/bloom/v3"

	"github.com/jakub-galecki/godb/internal/cache"
	"github.com/jakub-galecki/godb/log"
)

type SST struct {
	sstId string
	bf    *bloom.BloomFilter
	idx   *index
	fref  *os.File

	meta       *tableMeta
	blockCache cache.Cacher[[]byte]
	fsz        int64

	logger *log.Logger
}

func (s *SST) GetTableMeta() *tableMeta {
	return s.meta
}

func (s *SST) GetId() string {
	return s.sstId
}

func (s *SST) GetPath() string {
	return s.fref.Name()
}

func (s *SST) GetMin() []byte {
	if s.meta == nil {
		return nil
	}
	return s.meta.min
}
func (s *SST) GetMax() []byte {
	if s.meta == nil {
		return nil
	}
	return s.meta.max
}

func (s *SST) GetFileSize() int64 {
	return s.fsz
}
