package wal

import (
	"fmt"
	"github.com/jakub-galecki/godb/common"
	"strconv"
)

type WalLogNum uint64

func WalLogNumFromString(n string) (WalLogNum, bool) {
	wLogSeq, err := strconv.ParseUint(n, 10, 64)
	if err != nil {
		return 0, false
	}
	return WalLogNum(wLogSeq), true
}

func (n WalLogNum) String() string {
	return fmt.Sprintf("%09d", n)
}

func (n WalLogNum) FileName() string {
	return fmt.Sprintf("%s.log", n.String())
}

type WalIteratorResult struct {
	Op    common.DbOp
	Key   []byte
	Value []byte
}

/*
NOTE

	For now this is the simple implementation with single log file without
	any segments.
*/
