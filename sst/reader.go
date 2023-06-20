package sst

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"godb/bloom"

	"github.com/vmihailenco/msgpack/v5"
)

type reader struct {
	dirPath   string
	bf        bloom.Filter
	bloomFile *os.File
	idxFile   *os.File
	dbFile    *os.File
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

	idxPath := filepath.Join(opts.dirPath, IndexFName)
	r.idxFile, err = os.Open(idxPath)
	if err != nil {
		return nil, err
	}

	dbPath := filepath.Join(opts.dirPath, DBFName)
	r.dbFile, err = os.Open(dbPath)
	if err != nil {
		return nil, err
	}

	// r.bf, err = r.internalReadBloomFilter()
	// if err != nil {
	// 	return nil, err
	// }

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
	return r.getOffsetFromIndex(key) != -1
}

func (r *reader) Get(key []byte) ([]byte, error) {
	if !r.Contains(key) {
		return nil, errors.New("not found")
	}

	offset := r.getOffsetFromIndex(key)

	if _, err := r.dbFile.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}

	bs := make([]byte, 8)
	_, err := r.dbFile.Read(bs)
	if err != nil {
		return nil, err
	}
	lenght := decodeNum(bs)

	data := make([]byte, lenght)
	_, err = r.dbFile.Read(data)
	if err != nil {
		return nil, err
	}

	entry, err := decode(data)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(entry.key, key) {
		return entry.value, nil
	}
	return nil, errors.New("not found")
}

// func (r *reader) internalReadBloomFilter() (bloom.Filter, error) {
// 	filterPath := filepath.Join(r.dirPath, BloomFName)
// 	if _, err := os.Stat(filterPath); errors.Is(err, os.ErrNotExist) {
// 		return nil, fmt.Errorf("bloom filter file not exists") // should we return error ???
// 	}
// 	bf := bloom.NewFilter(bloom.MaxSize)
// 	filterFile, err := os.Open(filterPath)
// 	r.bloomFile = filterFile

// 	if err != nil {
// 		return nil, err
// 	}

// 	if err := bf.Read(filterFile); err != nil {
// 		return nil, err
// 	}
// 	return bf, nil
// }

func (r *reader) Close() error {
	if r.bloomFile != nil {
		return r.bloomFile.Close()
	}
	return nil
}

func (r *reader) getOffsetFromIndex(key []byte) int {
	for {
		bs := make([]byte, 8)
		_, err := r.idxFile.Read(bs)
		if err != nil {
			return -1
		}
		lenght := decodeNum(bs)

		data := make([]byte, lenght)
		_, err = r.idxFile.Read(data)
		if err != nil {
			return -1
		}

		entry, err := decode(data)
		if err != nil {
			return -1
		}

		if bytes.Equal(key, entry.key) {
			return decodeNum(entry.value)
		}
	}
}

// func (r *reader) internalReadSparseIndex() (sparse.Index, error) {
// 	sparseIndexPath := filepath.Join(r.dirPath, SparseIndexFName)
// 	if _, err := os.Stat(sparseIndexPath); errors.Is(err, os.ErrNotExist) {
// 		return nil, fmt.Errorf("bloom filter file not exists") // should we return error ???
// 	}
// 	return
// }

func decode(data []byte) (*penc, error) {
	var p penc
	if err := msgpack.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func decodeNum(n []byte) int {
	return int(binary.BigEndian.Uint64(n))
}
