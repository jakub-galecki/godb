package sst

import (
	"godb/vfs"
	"sync"
	"time"
)

type Builder interface {
	Add([]byte, []byte) Builder
	Finish() SST
}

func NewBuilder(fpath string) Builder {
	bdr := &builder{
		offset:      0,
		readyBlocks: make(chan *block),
		file:        vfs.NewVFS[block](fpath, F_FLAGS, F_PERMISSION),
	}

	bdr.done.Add(1)
	return bdr
}

type builder struct {
	currentBlock *block
	offset       int

	filePath string
	file     vfs.VFS[block]

	readyBlocks chan *block
	done        sync.WaitGroup
}

func (bdr *builder) Add(key, value []byte) Builder {
	if size := bdr.currentBlock.getSize(); size >= BLOCK_SIZE {
		bdr.offset += size
		bdr.readyBlocks <- bdr.currentBlock
		bdr.currentBlock = newBlock(bdr.offset)
	}
	_ = bdr.currentBlock.add(newEntry(key, value))
	return bdr
}

func (bdr *builder) Finish() SST {
	if bdr.currentBlock.getSize() > 0 {
		bdr.readyBlocks <- bdr.currentBlock
	}
	close(bdr.readyBlocks)
	bdr.done.Wait()

	return sst{
		table:   "",
		tableId: 0,
		bf:      nil,
		blocks:  nil,
	}
}

func (bdr *builder) readyBlocksWorker() {
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
