package common

import "errors"

var TOMBSTONE []byte

var (
	EndOfIterator = errors.New("out of records")
)
