package main

import (
	"encoding/binary"
	"godb/common"
	"sync"
	"sync/atomic"
)

var batchPool = sync.Pool{New: func() interface{} { return new(Batch) }}

const (
	headerLen = 12 // 8 bytes for seqNum and 4 bytes for count
)

type Batch struct {
	header     []byte
	buf        []byte
	committed  atomic.Bool
	forceFlush bool
	seqNum     uint64
	size       uint32
	off        uint64
}

func newBatch() *Batch {
	b := batchPool.Get().(*Batch)
	b.committed = atomic.Bool{}
	clear(b.buf)
	clear(b.header)
	b.seqNum = 0
	b.size = 0
	b.off = 0
	return b
}

func (b *Batch) release() {
	batchPool.Put(b)
}

func (b *Batch) Set(key, value []byte) *Batch {
	b.add(common.SET, key, value)
	b.size++
	return b
}

func (b *Batch) Delete(key []byte) *Batch {
	b.add(common.DELETE, key, nil)
	b.size++
	return b
}

func (b *Batch) add(op common.DbOp, key, value []byte) {
	keyLen, valueLen := len(key), len(value)
	need := 2*binary.MaxVarintLen64 + keyLen + valueLen + 1
	b.grow(need)
	copy(b.buf[b.off:], []byte{op})
	b.off += 1
	written := binary.PutUvarint(b.buf[b.off:], uint64(keyLen))
	b.off += uint64(written)
	copy(b.buf[b.off:], key)
	b.off += uint64(keyLen)
	if value == nil {
		return
	}
	written = binary.PutUvarint(b.buf[b.off:], uint64(valueLen))
	b.off += uint64(written)
	copy(b.buf[b.off:], value)
	b.off += uint64(valueLen)
}

func (b *Batch) grow(n int) {
	nSize := n + len(b.buf)
	if nSize > cap(b.buf) {
		newSlice := make([]byte, (n+len(b.buf))*2)
		copy(newSlice, b.buf)
		b.buf = newSlice
	}
	b.buf = b.buf[:nSize]
}

func (b *Batch) encode() []byte {
	if b.header == nil {
		b.header = make([]byte, headerLen)
	}
	n := binary.PutUvarint(b.header, b.seqNum)
	binary.LittleEndian.PutUint32(b.header[n:], b.size)
	return append(b.header, b.buf[:b.off]...)
}

func (b *Batch) decodeHeader(raw []byte) {
	var read int
	if len(raw) < headerLen {
		panic("cannot decode as slice is smaller than header length")
	}
	header := raw[:headerLen]
	b.seqNum, read = binary.Uvarint(header)
	b.size = binary.LittleEndian.Uint32(header[read:])
}

func (b *Batch) Size() int {
	return int(b.size)
}

func (b *Batch) decode(raw []byte) {
	b.decodeHeader(raw)
	b.buf = raw[headerLen:]
	b.off = 0
}

func DecodeBatch(raw []byte) *Batch {
	b := newBatch()
	b.decode(raw)
	b.off = uint64(len(b.buf))
	return b
}

type batchIter struct {
	off    uint64
	total  uint64
	buf    []byte
	seqNum uint64
}

func (b *Batch) Iter() *batchIter {
	return &batchIter{
		off:    0,
		total:  b.off,
		buf:    b.buf[:b.off],
		seqNum: b.seqNum,
	}
}

func (b *batchIter) Next() (common.DbOp, uint64, []byte, []byte) {
	if b.off >= uint64(len(b.buf)) || b.off >= b.total {
		return 0, 0, nil, nil
	}
	op := b.buf[b.off]
	seq := b.seqNum
	b.seqNum++
	if op == common.DELETE {
		b.off += 1
		keyLen, read := binary.Uvarint(b.buf[b.off:])
		b.off += uint64(read)
		key := make([]byte, keyLen)
		copy(key, b.buf[b.off:b.off+keyLen])
		b.off += keyLen
		return op, seq, key, nil
	}
	b.off += 1
	keyLen, read := binary.Uvarint(b.buf[b.off:])
	b.off += uint64(read)
	key := make([]byte, keyLen)
	copy(key, b.buf[b.off:b.off+keyLen])
	b.off += keyLen
	valueLen, read := binary.Uvarint(b.buf[b.off:])
	b.off += uint64(read)
	value := make([]byte, valueLen)
	copy(value, b.buf[b.off:b.off+valueLen])
	b.off += valueLen
	return op, seq, key, value
}
