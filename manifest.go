package main

import (
	"errors"
	"godb/common"
	"os"
	"path"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

type manifest struct {
	f                 *os.File
	Id                string           `msgpack:"id"`
	L0                []string         `msgpack:"l0"`     // id's of the sst files
	Levels            map[int][]string `msgpack:"levels"` // id's of the sst files
	Table             string           `msgpack:"table"`
	CreatedAt         int64            `msgpack:"created_at"`
	Path              string           `msgpack:"path"`
	BlockSize         int              `msgpack:"block_size"`
	NLevels           int              `msgpack:"n_levels"`
	MaxLevels         int              `msgpack:"max_levels"`
	LastFlushedSeqNum uint64           `msg:"unflushed_log_seq"`
	// seqNum is a global counter for wal and memtable to distinguish between flushed and
	// unflushed entries. Inspired by pebble approach
	SeqNum uint64 `msgpack:"seq_num"`
}

func newManifest(id string, dir, table string, blockSize, maxLevels int) (*manifest, error) {
	mFile, err := common.CreateFile(path.Join(dir, common.MANIFEST))
	if err != nil {
		return nil, err
	}

	m := &manifest{
		Id:        id,
		f:         mFile,
		L0:        []string{},
		Levels:    make(map[int][]string),
		Table:     table,
		CreatedAt: time.Now().UnixNano(),
		Path:      dir,
		BlockSize: blockSize,
		NLevels:   0,
		MaxLevels: maxLevels,
	}

	return m, nil
}

func readManifest(dir string) (*manifest, error) {
	f, err := os.Open(path.Join(dir, common.MANIFEST))
	if err != nil {
		return nil, err
	}
	dec := msgpack.NewDecoder(f)
	m := &manifest{
		f: f,
	}
	err = dec.Decode(&m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (m *manifest) addSst(level int, sstId string) {
	if level == 0 {
		m.L0 = append(m.L0, sstId)
		return
	}
	m.Levels[level] = append(m.Levels[level], sstId)
}

func (m *manifest) fsync() error {
	b, err := msgpack.Marshal(m)
	if err != nil {
		return err
	}
	n, err := m.f.Write(b)
	if err != nil {
		return err
	}
	if n == 0 {
		return errors.New("manifest: written zero bytes")
	}
	return nil
}
