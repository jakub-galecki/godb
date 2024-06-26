package sst

import (
	"encoding/binary"
	"os"
	"path"
	"time"

	"godb/log"
	"godb/vfs"

	"github.com/bits-and-blooms/bloom/v3"
)

type Builder interface {
	Add([]byte, []byte) Builder
	Finish() *SST
}

type builder struct {
	currentBlock *block
	offset       uint64
	size         uint64
	dir          string
	file         vfs.VFS[block]
	bf           *bloom.BloomFilter
	index        *indexBuilder // one block should be enough for now but should be changes
	sstId        string
	logger       *log.Logger
}

func NewBuilder(logger *log.Logger, dir string, n int, id string) Builder {
	bdr := &builder{
		offset:       0,
		dir:          dir,
		file:         vfs.NewVFS[block](dir, id+".db", F_FLAGS, F_PERMISSION),
		index:        newBuilderIndex(),
		bf:           bloom.NewWithEstimates(uint(n), 0.01),
		currentBlock: newBlock(),
		sstId:        id,
		logger:       logger,
	}
	return bdr
}

func (bdr *builder) Add(key, value []byte) Builder {
	//start := time.Now()
	entry := newEntry(key, value)
	// ensure that written block size will not be greater than BLOCK_SIZE
	if size := entry.getSize() + bdr.currentBlock.getSize(); size > BLOCK_SIZE {
		// logger.Debug("SST::BUILDER::ADD block size > BLOCK_SIZE %d", size)
		bdr.flushBlock(bdr.currentBlock)
		bdr.currentBlock = newBlock()
	}
	err := bdr.currentBlock.add(entry)
	if err != nil {
		panic(err)
	}
	bdr.bf.Add(key)
	//bdr.logger.Event("sstBuilder.Add", start)
	return bdr
}

func (bdr *builder) Finish() *SST {
	var (
		meta  = tableMeta{}
		start = time.Now()
	)
	if bdr.currentBlock.getSize() > 0 {
		bdr.flushBlock(bdr.currentBlock)
	}
	// data info
	meta.dataOffset = 0
	meta.dataSize = bdr.size

	bfSize, err := bdr.bf.WriteTo(bdr.file)
	//logger.Debugf("SST::FINISH bfsize: %d", bfSize)
	if err != nil {
		panic(err)
	}

	// meta
	meta.bfOffset = bdr.offset
	meta.bfSize = uint64(bfSize)
	bdr.offset += uint64(bfSize)

	// index
	meta.indexOffset = bdr.offset
	n, err := bdr.file.Write(bdr.index.buf.Bytes())
	if err != nil {
		panic(err)
	}

	meta.indexSize = uint64(n)
	bdr.offset += uint64(n)

	if err := meta.writeTo(bdr.file); err != nil {
		panic(err)
	}

	if err := bdr.file.Flush(); err != nil {
		panic(err)
	}

	err = bdr.file.GetFileRef().Close()
	if err != nil {
		// trace.Error().Err(err).Msg("closing file after SST builder finish")
		panic(err)
	}

	fref, err := os.Open(path.Join(bdr.dir, bdr.sstId+".db"))
	if err != nil {
		// trace.Error().Err(err).Msg("opeing file for read after SST builder finish")
		panic(err)
	}
	bdr.logger.Event("sstBuilder.Finish", start)
	return &SST{
		meta:  meta,
		bf:    bdr.bf,
		idx:   indexFromBuf(bdr.index.buf),
		fref:  fref,
		sstId: bdr.sstId,
	}
}

func (bdr *builder) addIndex(min []byte) {
	offset := make([]byte, 8)
	binary.BigEndian.PutUint64(offset, bdr.offset)
	if err := bdr.index.add(&entry{key: min, value: offset}); err != nil {
		panic(err)
	}
}

func (bdr *builder) flushBlock(b *block) {
	n, err := bdr.file.Write(b.buf.Bytes())

	bdr.addIndex(b.min)

	bdr.offset += uint64(n)
	bdr.size += uint64(n)
	if err != nil {
		bdr.logger.Error().Err(err).Msg("error while writing block to disk")
	}
}
