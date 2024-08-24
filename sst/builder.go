package sst

import (
	"errors"
	"os"
	"path"
	"time"

	"github.com/jakub-galecki/godb/common"
	"github.com/jakub-galecki/godb/log"
	"github.com/jakub-galecki/godb/vfs"

	"github.com/bits-and-blooms/bloom/v3"
)

type Builder interface {
	Add(*common.InternalKey, []byte) Builder
	Finish() *SST
}

type builder struct {
	curBB    *blockBuilder
	offset   uint64
	size     uint64
	dir      string
	file     vfs.VFS[block]
	bf       *bloom.BloomFilter
	index    *indexBuilder
	sstId    string
	logger   *log.Logger
	min, max []byte
}

func NewBuilder(logger *log.Logger, dir string, n uint64, id string) Builder {
	bdr := &builder{
		offset: 0,
		dir:    dir,
		index:  newBuilderIndex(),
		bf:     bloom.NewWithEstimates(uint(n), 0.01),
		curBB:  newBlockBuilder(),
		sstId:  id,
		logger: logger,
		file:   vfs.NewVFS[block](dir, id+".db", F_FLAGS, F_PERMISSION),
	}
	return bdr
}

func (bdr *builder) Add(key *common.InternalKey, value []byte) Builder {
	e := &entry{key: key.Serialize(), value: value, rawKey: key}
	err := bdr.curBB.add(e)
	if err != nil && !errors.Is(err, errNoSpaceInBlock) {
		panic(err)
	}
	if errors.Is(err, errNoSpaceInBlock) {
		toFlush, mink, maxk := bdr.curBB.rotateBlock()
		bdr.flushBlock(toFlush, mink, maxk)
		if err := bdr.curBB.add(e); err != nil {
			// we just created new block, so there should be not errNoSpaceInBlock
			panic(err)
		}
	}
	bdr.bf.Add(key.UserKey)
	return bdr
}

func (bdr *builder) Finish() *SST {
	var (
		meta  = newTableMeta()
		start = time.Now()
	)

	if bdr.curBB.cur.getSize() > 0 {
		b, mink, maxk := bdr.curBB.finish()
		bdr.flushBlock(b, mink, maxk)
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
	n, err := bdr.file.Write(bdr.index.buf[:bdr.index.off])
	if err != nil {
		panic(err)
	}

	meta.indexSize = uint64(n)
	bdr.offset += uint64(n)

	meta.keysInfoOffset = bdr.offset
	meta.initKeysInfo(bdr.min, bdr.max)
	n, err = meta.encodeKeysInfo(bdr.file)
	if err != nil {
		panic(err)
	}
	meta.keysInfoSize = uint64(n)

	if err := meta.writeTo(bdr.file); err != nil {
		panic(err)
	}

	if err := bdr.file.Flush(); err != nil {
		panic(err)
	}

	err = bdr.file.GetFileRef().Close()
	if err != nil {
		bdr.logger.Error().Err(err).Msg("closing file after SST builder finish")
		panic(err)
	}

	fref, err := os.Open(path.Join(bdr.dir, bdr.sstId+".db"))
	if err != nil {
		bdr.logger.Error().Err(err).Msg("opeing file for read after SST builder finish")
		panic(err)
	}
	st, _ := fref.Stat()
	bdr.logger.Event("sstBuilder.Finish", start)
	return &SST{
		meta:   meta,
		bf:     bdr.bf,
		idx:    indexFromBuf(bdr.index.buf[:bdr.index.off]),
		fref:   fref,
		sstId:  bdr.sstId,
		logger: bdr.logger,
		fsz:    st.Size(),
	}
}

func (bdr *builder) addIndex(minKey []byte) {
	if err := bdr.index.add(minKey, bdr.offset); err != nil {
		panic(err)
	}
}

func (bdr *builder) flushBlock(b *block, minKey, maxKey []byte) {
	bdr.updateMinMax(minKey, maxKey)
	n, err := bdr.file.Write(b.buf)
	bdr.addIndex(minKey)
	bdr.offset += uint64(n)
	bdr.size += uint64(n)
	if err != nil {
		bdr.logger.Error().Err(err).Msg("error while writing block to disk")
	}
}

func (bdr *builder) updateMinMax(minKey, maxKey []byte) {
	if len(bdr.min) == 0 || len(bdr.max) == 0 {
		bdr.min = minKey
		bdr.max = maxKey
		return
	}
	bdr.min = common.Min(bdr.min, minKey)
	bdr.max = common.Max(bdr.max, maxKey)
}

func (bdr *builder) GetSize() uint64 {
	return bdr.size
}
