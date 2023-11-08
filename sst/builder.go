package sst

import (
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

func NewBuilder(table string, n int) Builder {
	fpath := fmt.Sprintf("./%s.db", table)
	bdr := &builder{
		offset:       0,
		readyBlocks:  make(chan *block),
		filePath:     fpath,
		file:         vfs.NewVFS[block](fpath, F_FLAGS, F_PERMISSION),
		bf:           *bloom.NewWithEstimates(uint(n), 0.01),
		currentBlock: newBlock(0),
	}
	bdr.done.Add(1)
	go bdr.readyBlockWorker()
	return bdr
}

type builder struct {
	currentBlock *block
	offset       uint64
	size         uint64
	filePath     string
	file         vfs.VFS[block]
	bf           bloom.BloomFilter
	readyBlocks  chan *block
	done         sync.WaitGroup
}

func (bdr *builder) Add(key, value []byte) Builder {
	if size := bdr.currentBlock.getSize(); size >= BLOCK_SIZE {
		bdr.offset += uint64(size)
		bdr.size += uint64(size)

		bdr.readyBlocks <- bdr.currentBlock
		bdr.currentBlock = newBlock(bdr.offset)
	}
	_ = bdr.currentBlock.add(newEntry(key, value))
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
	if err != nil {
		panic(err)
	}

	// meta
	meta.bfOffset += uint64(bdr.offset)
	meta.bfSize = uint64(bfSize)
	bdr.offset += uint64(bfSize)

	meta.writeTo(bdr.file)

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
	select {
	case blk, ok := <-bdr.readyBlocks:
		if !ok {
			bdr.done.Done()
			break
		}
		bdr.flushBlock(blk)
	default:
		time.Sleep(10 * time.Millisecond)
	}
}

func (bdr *builder) flushBlock(b *block) {
	_, err := bdr.file.Write(b.buf.Bytes())
	if err != nil {
		logger.Error("Error while writing block to disk", err)
	}
}
