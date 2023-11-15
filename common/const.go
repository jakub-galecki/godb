package common

import "errors"

var TOMBSTONE []byte

var (
	EndOfIterator = errors.New("out of records")
	KeyNotFound   = errors.New("key not found")
)

const (
	MAX_MEMTABLE_THRESHOLD = 8 * 1024

	MAX_SINK_SIZE = 8
)
