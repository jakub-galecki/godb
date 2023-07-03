package sst

const (
	// Maximum block size. When it reaches this size it will be flushed to disk
	BLOCK_SIZE = 1 << 12

	F_PREFIX = "data_block.bin"
)

type Block interface {
	Get([]byte) ([]byte, error)
}

type block struct {
	min []byte
	max []byte

	buf []byte

	offset uint
}

func newBlock() *block {
	return &block{}
}

func (b *block) clearBuf() {
	b.buf = make([]byte, 0)
}
