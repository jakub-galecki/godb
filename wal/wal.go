package wal

import (
	"fmt"
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
    Op int 
    Key []byte 
    Value []byte
}

func walItResFromBytes(b []byte) (*WalIteratorResult, error) {
    var (
        op int 
        key string 
        value string 
    )
    
    _, err := fmt.Sscanf(string(b), "%d %s %s", &op, &key, &value)
    if err != nil {
        return nil, err 
    }

    return &WalIteratorResult{op, []byte(key), []byte(value)}, nil 
}

/*
NOTE

	For now this is the simple implementation with single log file without
	any segments.
*/
