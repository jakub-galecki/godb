package common

import "errors"

var TOMBSTONE []byte

var (
	EndOfIterator = errors.New("out of records")
	KeyNotFound   = errors.New("key not found")
	ErrPathFile   = errors.New("provied path points to a file instead of directory")
)

const (
	MAX_MEMTABLE_THRESHOLD = 1 << 10

	MAX_SINK_SIZE = 8
)
