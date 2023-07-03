package common

import "errors"

var TOMBSTONE []byte

var (
	EndOfIterator = errors.New("out of records")
)

const (
	MAX_MEMTABLE_THRESHOLD = 8 * 1024
)
