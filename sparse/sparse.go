package sparse

import (
	"io"
)

type Index interface {
	Get([]byte) int
	Read(io.Reader)
	Write(io.Writer)
	// FromMemTable(mem *memtable.MemTable)
}
