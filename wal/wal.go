package wal

import (
	"fmt"
)

type WalLogNum uint64

func (n WalLogNum) String() string {
	return fmt.Sprintf("%09d", n)
}

/*
NOTE

	For now this is the simple implementation with single log file without
	any segments.
*/
