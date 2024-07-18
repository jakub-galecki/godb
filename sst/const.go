package sst

import (
	"errors"
	"os"
)

const (
	F_PERMISSION = 0600
	F_FLAGS      = os.O_WRONLY | os.O_CREATE | os.O_TRUNC | os.O_APPEND
	F_READ       = os.O_RDONLY
)

var (
	ErrNotFoundInBloom = errors.New("key not found in bloom filter")
)
