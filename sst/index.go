package sst

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

var (
	zeroIdx = &index{}
)

type indexBuilder struct {
	buf  *bytes.Buffer
	size int
}

func newBuilderIndex() *indexBuilder {
	return &indexBuilder{
		buf: new(bytes.Buffer),
	}
}

func (i *indexBuilder) add(e *entry) error {
	n, err := encode(e, i.buf)
	if err != nil {
		return err
	}
	i.size += n
	return nil
}

type indexEntry struct {
	key     []byte
	foffset uint64
	blength int
}

type index struct {
	off []*indexEntry
}

func indexFromBuf(buf *bytes.Buffer) *index {
	var (
		idx    = index{}
		tmpEnt = entry{}

		err error
		n   int
	)

	curLen, bufLen := 0, buf.Len()

	for n, err = decode(buf, &tmpEnt); err == nil; n, err = decode(buf, &tmpEnt) {
		if len(tmpEnt.key) == 0 && len(tmpEnt.value) == 0 {
			break
		}
		idx.off = append(idx.off, &indexEntry{key: tmpEnt.key, foffset: binary.BigEndian.Uint64(tmpEnt.value)})
		// logger.Debugf("decoded entry key: [%s], value [%s]", tmpEnt.key, tmpEnt.value)

		curLen += n
		if curLen >= bufLen {
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
		return nil, fmt.Errorf("not found in sparse index")
	}

	return i.off[idx], nil
}
