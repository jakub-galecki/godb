package common

// inspired by https://github.com/cockroachdb/pebble/blob/master/internal/base/internal.go

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"math"
)

type KeyMeta uint64

type InternalKey struct {
	UserKey []byte
	Meta    KeyMeta
}

func SearchInternalKey(key []byte) *InternalKey {
	return &InternalKey{key, KeyMeta(math.MaxUint64)}
}

func NewInternalKey(ukey []byte, seqNum uint64, kind uint8) *InternalKey {
	return &InternalKey{
		UserKey: ukey,
		Meta:    makeMeta(seqNum, kind),
	}
}

func makeMeta(seqNum uint64, kind uint8) KeyMeta {
	return KeyMeta((seqNum << 8) | uint64(kind))
}

func (ik *InternalKey) GetMeta() KeyMeta {
	return ik.Meta
}

func (ik *InternalKey) Serialize() []byte {
	buf := make([]byte, len(ik.UserKey)+64)
	n := copy(buf, ik.UserKey)
	binary.BigEndian.PutUint64(buf[n:], uint64(ik.Meta))
	return buf
}

func (ik *InternalKey) Compare(other *InternalKey) int {
	if ik == nil {
		return -1
	}
	ukeyCmp := bytes.Compare(ik.UserKey, other.UserKey)
	if ukeyCmp != 0 {
		return ukeyCmp
	}
	/*
	   If userKeys are the same then we are comparing sequenceNumbers
	   lower sequence number means that the key is greater.

	   a = InternalKey{UserKey: "abc", meta: 500}
	   b = InternalKey{UserKey: "abc", meta: 501}
	   a < b
	*/
	return cmp.Compare(other.Meta, ik.Meta)
}

func (ik *InternalKey) Equal(other *InternalKey) bool {
	return ik.Compare(other) == 0
}

func (ik *InternalKey) GetSize() int {
	return len(ik.UserKey) + 64
}
