package sst

import (
	"encoding/binary"
	"fmt"
	"github.com/jakub-galecki/godb/common"
)

func encode(e *entry, dst []byte) (int, error) {
	return e.encode(dst)
}

func decode(src []byte, e *entry) (int, error) {
	return e.decode(src)
}

type entry struct {
	rawKey *common.InternalKey
	key    []byte
	value  []byte
}

func newEntry(key, value []byte) *entry {
	return &entry{
		key:   key,
		value: value,
	}
}

func (e *entry) encode(dst []byte) (int, error) {
	off := 0
	keyLen, valueLen := len(e.key), len(e.value)
	n := binary.PutUvarint(dst[:], uint64(keyLen))
	off += n

	copy(dst[off:], e.key)
	off += keyLen

	n = binary.PutUvarint(dst[off:], uint64(valueLen))
	off += n

	copy(dst[off:], e.value)
	off += valueLen

	return off, nil
}

func (e *entry) decode(src []byte) (int, error) {
	if e == nil {
		return 0, fmt.Errorf("nil entry")
	}
	var (
		off              = 0
		n                int
		keyLen, valueLen uint64
	)

	keyLen, n = binary.Uvarint(src)
	if keyLen == 0 {
		return -1, errNoMoreData
	}
	off += n

	key := make([]byte, keyLen)
	copy(key, src[off:off+int(keyLen)])
	off += int(keyLen)

	valueLen, n = binary.Uvarint(src[off:])
	off += n

	value := make([]byte, valueLen)
	copy(value, src[off:off+int(valueLen)])
	off += int(valueLen)

	e.key = key
	e.value = value
	return off, nil
}

func (e *entry) getSize() uint64 {
	return uint64(2*binary.MaxVarintLen64 + len(e.key) + len(e.value))
}
