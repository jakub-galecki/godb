package main

import (
	"godb/common"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/tinylib/msgp/msgp"
)

//go:generate msgp

type Manifest struct {
	f                 *os.File
	mu                sync.Mutex
	Id                string     `msgp:"id"`
	L0                []string   `msgp:"l0"`     // id's of the sst files
	Levels            [][]string `msgp:"levels"` // id's of the sst files
	Table             string     `msgp:"table"`
	CreatedAt         int64      `msgp:"created_at"`
	Path              string     `msgp:"path"`
	BlockSize         int        `msgp:"block_size"`
	LevelCount        int        `msgp:"level_count"`
	MaxLevels         int        `msgp:"max_levels"`
	LastFlushedSeqNum uint64     `msgp:"unflushed_log_seq"`
	// seqNum is a global counter for wal and memtable to distinguish between flushed and
	// unflushed entries. Inspired by pebble approach
	SeqNum uint64 `msgp:"seq_num"`
}

func newManifest(id string, dir, table string, blockSize, maxLevels int) (*Manifest, error) {
	mFile, err := common.CreateFile(path.Join(dir, common.MANIFEST))
	if err != nil {
		return nil, err
	}

	m := &Manifest{
		Id:         id,
		f:          mFile,
		L0:         []string{},
		Levels:     make([][]string, 0, maxLevels),
		Table:      table,
		CreatedAt:  time.Now().UnixNano(),
		Path:       dir,
		BlockSize:  blockSize,
		LevelCount: 0,
		MaxLevels:  maxLevels,
	}

	return m, nil
}

func readManifest(dir string) (*Manifest, error) {
	m := &Manifest{}
	f, err := os.Open(path.Join(dir, common.MANIFEST))
	if err != nil {
		return nil, err
	}
	err = m.DecodeMsg(msgp.NewReader(f))
	if err != nil {
		if err := f.Close(); err != nil {
			return nil, err
		}
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	f, err = os.OpenFile(path.Join(dir, common.MANIFEST), os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	m.f = f
	return m, nil
}

func (m *Manifest) addSst(level int, sstId string) {
	if level == 0 {
		m.L0 = append(m.L0, sstId)
		return
	}
	m.Levels[level] = append(m.Levels[level], sstId)
}

// This should be somehow optimized because now we have to remove file content and write new Manifest
func (m *Manifest) fsync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.f.Truncate(0); err != nil {
		return err
	}
	if _, err := m.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	w := msgp.NewWriter(m.f)
	err := m.EncodeMsg(w)
	if err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := m.f.Sync(); err != nil {
		return err
	}
	return nil
}
