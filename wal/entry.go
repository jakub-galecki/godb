package wal

import (
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

type WalEntry interface {
	Serialize() []byte
}

type Entry struct {
	Timestamp time.Time
	Lsn       uint32
	PrevLsn   uint32
	TxId      uint32
	Size      uint32
	Data      []byte
}

func NewEntry(lsn, prevLsn, txId, size uint32, data []byte) *Entry {
	var (
		timestamp = time.Now()
	)
	return &Entry{
		Timestamp: timestamp,
		Lsn:       lsn,
		PrevLsn:   prevLsn,
		TxId:      txId,
		Size:      size,
		Data:      data,
	}
}

func (e *Entry) Serialize() []byte {
	xs, err := msgpack.Marshal(*e)
	if err != nil {
		return nil
	}
	xs = append(xs, '\n')
	return xs
}
