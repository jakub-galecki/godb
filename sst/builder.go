package sst

type Builder interface {
	Add([]byte, []byte) Builder
	Finish() blocks
}

func NewBuilder() Builder {
	return &builder{
		offset:      0,
		readyBlocks: newBlockGroup(),
	}
}

type builder struct {
	readyBlocks  blocks
	currentBlock *block
	offset       int
}

func (b *builder) Add(key, value []byte) Builder {
	if size := b.currentBlock.getSize(); size >= BLOCK_SIZE {
		b.offset += size
		b.readyBlocks.add(b.currentBlock)
		b.currentBlock = newBlock(b.offset)
	}

	b.currentBlock.add(newEntry(key, value))

	return b
}

func (b *builder) Finish() blocks {
	if b.currentBlock.getSize() > 0 {
		b.readyBlocks.add(b.currentBlock)
	}
	return b.readyBlocks
}
