package sst

import (
	"os"

	"github.com/bits-and-blooms/bloom/v3"

	"godb/internal/cache"
	"godb/log"
)

type SST struct {
	sstId string
	bf    *bloom.BloomFilter
	idx   *index
	fref  *os.File

	meta       tableMeta
	blockCache *cache.Cache[[]byte]

	logger *log.Logger
}

func (s *SST) GetTableMeta() tableMeta {
	return s.meta
}

func (s *SST) GetId() string {
	return s.sstId
}
