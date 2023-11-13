package sst

import (
	"encoding/binary"
	"fmt"
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

	indexSize   uint64
	indexOffset uint64
}

func (tm *tableMeta) writeTo(w io.Writer) error {
	err := binary.Write(w, binary.BigEndian, tm.bfOffset)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.BigEndian, tm.bfSize)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.BigEndian, tm.dataOffset)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.BigEndian, tm.dataSize)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.BigEndian, tm.indexOffset)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.BigEndian, tm.indexSize)
	if err != nil {
		return err
	}

	return nil
}

func (tm *tableMeta) readFrom(r io.Reader) error {
	if tm == nil {
		return fmt.Errorf("aaa")
	}

	if err := binary.Read(r, binary.BigEndian, &tm.bfOffset); err != nil {
		return err
	}

	if err := binary.Read(r, binary.BigEndian, &tm.bfSize); err != nil {
		return err
	}

	if err := binary.Read(r, binary.BigEndian, &tm.dataOffset); err != nil {
		return err
	}

	if err := binary.Read(r, binary.BigEndian, &tm.dataSize); err != nil {
		return err
	}

	if err := binary.Read(r, binary.BigEndian, &tm.indexOffset); err != nil {
		return err
	}

	if err := binary.Read(r, binary.BigEndian, &tm.indexSize); err != nil {
		return err
	}

	return nil
}
