package common

// inspired by https://github.com/cockroachdb/pebble/blob/master/internal/base/internal.go

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"math"
)

type KeyMeta uint64

const MetaLen = 8

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
	buf := make([]byte, len(ik.UserKey)+MetaLen)
	n := copy(buf, ik.UserKey)
	binary.BigEndian.PutUint64(buf[n:], uint64(ik.Meta))
	return buf
}

func DeserializeKey(key []byte) *InternalKey {
	i := len(key) - MetaLen
	if i <= 0 {
		return nil
	}
	return &InternalKey{
		UserKey: key[:i:i],
		Meta:    KeyMeta(binary.BigEndian.Uint64(key[i:])),
	}

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

func (ik *InternalKey) SoftCompare(other *InternalKey) int {
	return bytes.Compare(ik.UserKey, other.UserKey)
}

func (ik *InternalKey) SoftEqual(other *InternalKey) bool {
	return bytes.Equal(ik.UserKey, other.UserKey)
}
func (ik *InternalKey) GetSize() int {
	return len(ik.UserKey) + MetaLen
}
