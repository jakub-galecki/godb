package sst

import (
	"encoding/binary"
	"io"
)

type blockMeta struct {
	offset uint64
	min    []byte
}

type tableMeta struct {
	bfOffset uint64
	bfSize   uint64

	dataSize   uint64
	dataOffset uint64

	// indexSize   uint64
	// indexOffset uint64
}

func (tm *tableMeta) writeTo(w io.Writer) error {
	binary.Write(w, binary.BigEndian, tm.bfOffset)
	binary.Write(w, binary.BigEndian, tm.bfSize)

	binary.Write(w, binary.BigEndian, tm.dataSize)
	binary.Write(w, binary.BigEndian, tm.dataOffset)

	// binary.Write(w, binary.BigEndian, tm.indexSize)
	// binary.Write(w, binary.BigEndian, tm.indexOffset)

	return nil
}

// func (tm *tableMeta) readFrom(r io.Reader) error {

// }
