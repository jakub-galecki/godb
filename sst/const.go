package sst

import (
	"os"
)

const (
	F_PERMISSION = 0600
	F_FLAGS      = os.O_WRONLY | os.O_CREATE | os.O_TRUNC | os.O_APPEND
)
