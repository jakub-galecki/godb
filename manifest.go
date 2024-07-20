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

// todo??: use protobuf
//go:generate msgp

type Manifest struct {
	f          *os.File
	mu         sync.Mutex
	Id         string
	L0         []string   // id's of the sst files
	Levels     [][]string // id's of the sst files
	Table      string
	CreatedAt  int64
	Path       string
	BlockSize  uint64
	LevelCount int
	MaxLevels  int
	// seqNum is a global counter for memtable writes to distinguish between new and old entries.
	SeqNum uint64
	// nextFileNumber indicates next file number that will be assigned to wal and memtable.
	NextFileNumber        uint64
	LastFlushedFileNumber uint64
}

func newManifest(id string, dir, table string, blockSize uint64, maxLevels int) (*Manifest, error) {
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
		SeqNum:     1,
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
