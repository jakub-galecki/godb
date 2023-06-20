package sst

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"godb/bloom"
	"godb/common"
	"godb/memtable"

	"github.com/vmihailenco/msgpack/v5"
)

type paths struct {
	idxPath         string
	dbPath          string // for now
	bloomFilterPath string
}

type writer struct {
	paths

	idx   *os.File
	db    *os.File
	bloom *os.File

	bf        bloom.Filter
	dirPath   string
	dbOffset  int
	idxOffset int
}

type WriterOpts struct {
	dirPath string
}

func NewWriter(opts *WriterOpts) (Writer, error) {
	var (
		err error
	)

	if opts == nil {
		return nil, fmt.Errorf("writer options are required")
	}

	if opts.dirPath == "" {
		return nil, fmt.Errorf("dirPath can not be empty")
	}

	var w writer
	w.dirPath = opts.dirPath
	w.idxPath = filepath.Join(opts.dirPath, IndexFName)
	w.idx, err = os.OpenFile(w.idxPath, F_FLAGS, F_PERMISSION)
	if err != nil {
		return nil, err
	}
	w.dbPath = filepath.Join(opts.dirPath, DBFName)
	w.db, err = os.OpenFile(w.dbPath, F_FLAGS, F_PERMISSION)
	if err != nil {
		return nil, err
	}

	w.bloomFilterPath = filepath.Join(opts.dirPath, BloomFName)
	w.bf = bloom.NewFilter(10000)
	return &w, nil
}

func (w *writer) Open() error {

	return nil
}

func (w *writer) Write(key, value []byte) error {
	var (
		err error

		dbWrittenBytes  = 0
		idxWrittenBytes = 0
	)

	encodedData, encodedLen, err := encode(key, value)
	if err != nil {
		return err
	}

	n, err := w.db.Write(encodedLen)
	if err != nil {
		return err
	}
	dbWrittenBytes += n

	n, err = w.db.Write(encodedData)
	if err != nil {
		return err
	}
	dbWrittenBytes += n

	encodedData, encodedLen, err = encode(key, encodeNum(w.dbOffset))
	if err != nil {
		return err
	}

	n, err = w.idx.Write(encodedLen)
	if err != nil {
		return err
	}
	idxWrittenBytes += n

	n, err = w.idx.Write(encodedData)
	if err != nil {
		return err
	}
	idxWrittenBytes += n

	w.dbOffset += dbWrittenBytes
	w.idxOffset += idxWrittenBytes
	w.bf.AddKey(key)
	return nil
}

func (w *writer) sync() error {
	if err := w.db.Sync(); err != nil {
		return err
	}
	if err := w.idx.Sync(); err != nil {
		return err
	}

	if err := w.bf.Write(w.bloom); err != nil {
		return err
	}

	if err := w.bloom.Sync(); err != nil {
		return err
	}

	return nil
}

func (w *writer) Close() error {
	if err := w.sync(); err != nil {
		return err
	}
	if err := w.db.Close(); err != nil {
		return err
	}
	if err := w.idx.Close(); err != nil {
		return err
	}
	return nil
}

func (w *writer) WriteMemTable(mem memtable.MemTable) error {
	it := mem.Iterator()
	for it.HasNext() {
		k, v, err := it.Next()
		fmt.Printf("%s %s\n", k, v)
		if err != nil {
			if errors.Is(err, common.EndOfIterator) {
				break
			}
		}

		if err := w.Write(k, v); err != nil {
			return err
		}
	}

	return nil
}

func encode(key, value []byte) ([]byte, []byte, error) {
	data := penc{
		key:   key,
		value: value,
	}
	encodedData, err := msgpack.Marshal(data)
	if err != nil {
		return nil, nil, err
	}

	return encodedData, encodeNum(len(encodedData)), nil
}

func encodeNum(n int) []byte {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, uint64(n))
	return bs
}
