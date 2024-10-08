package common

import "errors"

var TOMBSTONE []byte

var (
	EndOfIterator        = errors.New("out of records")
	ErrKeyNotFound       = errors.New("key not found")
	ErrPathFile          = errors.New("provied path points to a file instead of directory")
	ErrKeyAlreadyExists  = errors.New("key already exists")
	ErrIteratorExhausted = errors.New("iterator exhausted")
)

const (
	MAX_MEMTABLE_THRESHOLD = 1 * (1 << 20)

	MAX_SINK_SIZE = 8

	SST_DIR  = "sst"
	WAL      = "wal"
	MANIFEST = "MANIFEST"
)

type DbOp = uint8

const (
	SET DbOp = iota
	DELETE
)
