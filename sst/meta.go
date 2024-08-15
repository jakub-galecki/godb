package sst

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type keysInfo struct {
	min []byte
	max []byte
}

func (ki *keysInfo) encodeKeysInfo(w io.Writer) (int, error) {
	if ki == nil {
		return 0, errors.New("keysInfo is nil")
	}
	minLen, maxLen := len(ki.min), len(ki.max)
	buf := make([]byte, 2*binary.MaxVarintLen64+minLen+maxLen)

	innerOffset := binary.PutUvarint(buf, uint64(minLen))
	copy(buf[innerOffset:], ki.min)
	innerOffset += minLen

	innerOffset += binary.PutUvarint(buf[innerOffset:], uint64(maxLen))
	copy(buf[innerOffset:], ki.max)
	innerOffset += maxLen

	return w.Write(buf)
}

func (ki *keysInfo) decodeKeysInfo(raw []byte) uint64 {
	var off uint64

	minLen, n := binary.Uvarint(raw[off:])
	if n < 0 {
		return 0
	}
	off += uint64(n)
	ki.min = make([]byte, minLen)
	copy(ki.min, raw[off:off+minLen])
	off += minLen

	maxLen, n := binary.Uvarint(raw[off:])
	if n < 0 {
		return 0
	}
	off += uint64(n)

	ki.max = make([]byte, maxLen)
	copy(ki.max, raw[off:off+maxLen])
	off += maxLen

	return off
}

type tableMeta struct {
	*keysInfo

	bfOffset uint64
	bfSize   uint64

	dataSize   uint64
	dataOffset uint64

	indexSize   uint64
	indexOffset uint64

	keysInfoOffset uint64
	keysInfoSize   uint64
}

func newTableMeta() *tableMeta {
	return &tableMeta{
		keysInfo: &keysInfo{},
	}
}

func (tm *tableMeta) initKeysInfo(min, max []byte) {
	if tm.keysInfo == nil {
		tm.keysInfo = &keysInfo{min: min, max: max}
		return
	}
	tm.keysInfo.min = min
	tm.keysInfo.max = max
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

	err = binary.Write(w, binary.BigEndian, tm.keysInfoOffset)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.BigEndian, tm.keysInfoSize)
	if err != nil {
		return err
	}

	return nil
}

func (tm *tableMeta) readFrom(r io.Reader) error {
	if tm == nil {
		return fmt.Errorf("error reading table metadata")
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

	if err := binary.Read(r, binary.BigEndian, &tm.keysInfoOffset); err != nil {
		return err
	}

	if err := binary.Read(r, binary.BigEndian, &tm.keysInfoSize); err != nil {
		return err
	}

	return nil
}
