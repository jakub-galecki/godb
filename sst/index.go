package sst

import (
	"bytes"
	"encoding/binary"
	"github.com/jakub-galecki/godb/common"
)

var (
	zeroIdx = &index{}
)

type indexBuilder struct {
	buf  []byte
	off  uint64
	size int
}

func newBuilderIndex() *indexBuilder {
	return &indexBuilder{
		buf: make([]byte, BLOCK_SIZE),
	}
}

func (i *indexBuilder) add(key []byte, off uint64) error {
	e := &entry{key: key, value: make([]byte, binary.MaxVarintLen64)}
	i.grow(int(e.getSize()))
	binary.PutUvarint(e.value, off)

	n, err := encode(e, i.buf[i.off:])
	if err != nil {
		return err
	}
	i.off += uint64(n)
	i.size += n
	return nil
}

func (i *indexBuilder) grow(n int) {
	nSize := n + len(i.buf)
	if nSize > cap(i.buf) {
		newSlice := make([]byte, (n+len(i.buf))*2)
		copy(newSlice, i.buf)
		i.buf = newSlice
	}
	i.buf = i.buf[:nSize]
}

type indexEntry struct {
	key     []byte
	foffset uint64
	blength int
}

type index struct {
	off []*indexEntry
}

func indexFromBuf(buf []byte) *index {
	var (
		idx    = index{}
		tmpEnt = entry{}

		err    error
		n, off int
	)

	off, bufLen := 0, len(buf)
	for n, err = decode(buf[off:], &tmpEnt); err == nil; n, err = decode(buf[off:], &tmpEnt) {
		if len(tmpEnt.key) == 0 && len(tmpEnt.value) == 0 {
			break
		}
		foff, _ := binary.Uvarint(tmpEnt.value)
		idx.off = append(idx.off, &indexEntry{
			key:     tmpEnt.key,
			foffset: foff,
		})
		// logger.Debugf("decoded entry key: [%s], value [%s]", tmpEnt.key, tmpEnt.value)
		off += n
		if off >= bufLen {
			break
		}
	}

	if err != nil {
		panic(err)
	}
	return &idx
}

func (i *index) find(key []byte) (*indexEntry, error) {
	idx, low, up := -1, 0, len(i.off)-1
	for low <= up {
		mid := (low + up) / 2
		if cmp := bytes.Compare(i.off[mid].key, key); cmp <= 0 {
			low = mid + 1
			idx = mid
		} else {
			up = mid - 1
		}
	}

	if idx < 0 {
		return nil, common.ErrKeyNotFound
	}

	return i.off[idx], nil
}
