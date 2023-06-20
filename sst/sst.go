package sst

import "godb/memtable"

const (
	BloomFName       = "bloom.bin"
	SparseIndexFName = "sindex.bin"
	IndexFName       = "index.bin"
	DBFName          = "db.bin"
)

type penc struct {
	key   []byte `msgpack:"k"`
	value []byte `msgpack:"v"`
}

type Reader interface {
	Contains([]byte) bool
	Get([]byte) ([]byte, error)
	Close() error
}

type Writer interface {
	Open() error
	Write([]byte, []byte) error
	Close() error
	WriteMemTable(memtable.MemTable) error
}

type SST interface {
	Reader
	Writer
}

type sst struct {
	Reader
	Writer

	table string
}

func NewSST(table string) SST {
	var (
		s   sst
		err error
	)

	s.table = table

	s.Reader, err = NewReader(&ReaderOpts{
		dirPath: table,
	})

	if err != nil {
		panic(err)
	}

	s.Writer, err = NewWriter(&WriterOpts{
		dirPath: table,
	})
	if err != nil {
		panic(err)
	}

	return &s
}

func (s *sst) Close() error {
	if err := s.Reader.Close(); err != nil {
		return err
	}

	if err := s.Writer.Close(); err != nil {
		return err
	}

	return nil
}
