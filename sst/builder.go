package sst

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"godb/vfs"

	"github.com/bits-and-blooms/bloom"
)

type Builder interface {
	Add([]byte, []byte) Builder
	Finish() SST
}

type builder struct {
	currentBlock *block
	offset       uint64
	size         uint64
	filePath     string
	file         vfs.VFS[block]
	bf           *bloom.BloomFilter
	index        *block // one block should be enough for now but should be changes
	readyBlocks  chan *block
	done         sync.WaitGroup
}

func NewBuilder(table string, n int) Builder {
	fpath := fmt.Sprintf("./%s.db", table)
	bdr := &builder{
		offset:       0,
		readyBlocks:  make(chan *block),
		filePath:     fpath,
		file:         vfs.NewVFS[block](fpath, F_FLAGS, F_PERMISSION),
		index:        newBlock(),
		bf:           bloom.NewWithEstimates(uint(n), 0.01),
		currentBlock: newBlock(),
	}
	bdr.done.Add(1)
	go bdr.readyBlockWorker()
	return bdr
}

func (bdr *builder) Add(key, value []byte) Builder {

	if size := bdr.currentBlock.getSize(); size >= BLOCK_SIZE {
		logger.Debug("SST::BUILDER::ADD block size > BLOCK_SIZE %d", size)
		bdr.readyBlocks <- bdr.currentBlock
		bdr.currentBlock = newBlock()
	}
	err := bdr.currentBlock.add(newEntry(key, value))
	if err != nil {
		panic(err)
	}

	bdr.bf.Add(key)
	return bdr
}

func (bdr *builder) Finish() SST {
	var (
		meta = tableMeta{}
	)
	if bdr.currentBlock.getSize() > 0 {
		bdr.readyBlocks <- bdr.currentBlock
	}
	close(bdr.readyBlocks)
	bdr.done.Wait()

	// data info
	meta.dataOffset = 0
	meta.dataSize = bdr.size

	bfSize, err := bdr.bf.WriteTo(bdr.file)
	logger.Debugf("SST::FINISH bfsize: %d", bfSize)
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

	return &sst{
		table:   "",
		tableId: 0,
		meta:    meta,
	}
}

func (bdr *builder) readyBlockWorker() {
	//timer := time.NewTimer()
	for {
		select {
		case blk, ok := <-bdr.readyBlocks:
			if !ok {
				bdr.done.Done()
				return
			}
			bdr.flushBlock(blk)
		default:
			time.Sleep(10 * time.Millisecond)
		}
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
		logger.Error("Error while writing block to disk", err)
	}
}
