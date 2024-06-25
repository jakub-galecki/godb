package common

// inspired by https://github.com/cockroachdb/pebble/blob/master/internal/base/internal.go

import (
	"bytes"
	"cmp"
	"math"
)

type KeyMeta uint64

type InternalKey struct {
	UserKey []byte
	meta    KeyMeta
}

func SearchInternalKey(key []byte) *InternalKey {
	return &InternalKey{key, math.MaxUint64}
}

func NewInternalKey(ukey []byte, seqNum uint64, kind uint8) *InternalKey {
	return nil
}

func (ik *InternalKey) GetMeta() KeyMeta {
	return ik.meta
}

func (ik *InternalKey) Serialize() []byte {
	return nil
}

func (ik *InternalKey) Compare(other *InternalKey) int {
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
	return cmp.Compare(ik.meta, other.meta)
}
