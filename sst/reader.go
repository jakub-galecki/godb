package sst

import (
	"errors"
	"fmt"
	"godb/bloom"
	"os"
	"path/filepath"
)

type reader struct {
	dirPath string
	bf      bloom.Filter
}

type ReaderOpts struct {
	dirPath string
}

func NewReader(opts *ReaderOpts) (Reader, error) {
	var (
		r   reader
		err error
	)
	if opts.dirPath == "" {
		return nil, fmt.Errorf("no path to table provided")
	}
	r.dirPath = opts.dirPath

	r.bf, err = r.internalReadBloomFilter()
	if err != nil {
		return nil, err
	}

	return &r, nil
}

func (r *reader) Contains(key []byte) bool {
	if r.bf != nil {
		if inBloomFilter := r.bf.MayContain(key); !inBloomFilter {
			return false
		}
	}
	return r.internalContains(key)
}

func (r *reader) internalContains(key []byte) bool {
	return false
}

func (r *reader) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (r *reader) internalReadBloomFilter() (bloom.Filter, error) {
	filterPath := filepath.Join(r.dirPath, BloomFName)
	if _, err := os.Stat(filterPath); errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("bloom filter file not exists") // should we return error ???
	}
	bf := bloom.NewFilter(bloom.MaxSize)
	filterFile, err := os.Open(filterPath)

	if err != nil {
		return nil, err
	}

	if err := bf.Read(filterFile); err != nil {
		return nil, err
	}
	return bf, nil
}
