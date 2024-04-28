package wal

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

type Iterator struct {
	reader *bufio.Reader
}

func NewIterator(f io.Reader) (*Iterator, error) {
    r := bufio.NewReader(f)
	return &Iterator{
		reader: r,
	}, nil
}

func (it *Iterator) Next() (*WalIteratorResult, error) {
    line, _, err:= it.reader.ReadLine()
    if err != nil {
        return nil, err 
    }
    i := bytes.IndexByte(line, '|')
    if i < 0 {
        return nil, errors.New("wal delimiter not found")
    }
    return walItResFromBytes(line[i+1:])
}

func Iter(it *Iterator, f func(wr *WalIteratorResult) error) error {
    for {
        wr, err := it.Next()
        if errors.Is(err, io.EOF) {
            return nil
        } else if err != nil {
            return err 
        }

        if err := f(wr); err != nil {
            return err 
        }
    }
}
