package godb

import (
	"github.com/jakub-galecki/godb/sst"
	"sync/atomic"
)

type dbEnv struct {
	seqNum             atomic.Uint64
	lastFlushedFileNum atomic.Uint64
	nextFileNumber     atomic.Uint64
	l0                 []string
	levels             [][]string
}

func envFromManifest(m *Manifest) *dbEnv {
	env := &dbEnv{}
	env.seqNum.Store(m.SeqNum)
	env.lastFlushedFileNum.Store(m.LastFlushedFileNumber)
	env.l0 = make([]string, 0)
	env.levels = make([][]string, m.MaxLevels)
	env.nextFileNumber.Store(m.NextFileNumber)
	return env
}

func (env *dbEnv) refresh(m *Manifest) {
	env.seqNum.Store(m.SeqNum)
	env.lastFlushedFileNum.Store(m.LastFlushedFileNumber)
	env.nextFileNumber.Store(m.NextFileNumber)
	env.l0 = m.L0
	env.levels = m.Levels
}

func (env *dbEnv) getSeqNum(count int) uint64 {
	seq := env.seqNum.Add(uint64(count)) - uint64(count) + 1
	return seq
}

func (env *dbEnv) getNextFileNum() uint64 {
	seq := env.nextFileNumber.Add(1) - 1
	return seq
}

func (env *dbEnv) setLastFlushedSeqNum(fnum uint64) {
	if env.lastFlushedFileNum.Load() > fnum {
		panic("new last flushed file number is smaller")
	}
	env.lastFlushedFileNum.Store(fnum)
}

// requires to hold db lock
func (env *dbEnv) append(l int, tables ...*sst.SST) {
	for _, table := range tables {
		if l == 0 {
			env.l0 = append(env.l0, table.GetId())
		} else {
			env.levels[l-1] = append(env.levels[l-1], table.GetId())
		}
	}
}

// requires to hold db lock
func (env *dbEnv) remove(l int, tables ...*sst.SST) {
	remove := func(arr []string, id string) []string {
		for i, tableId := range arr {
			if tableId == id {
				return append(arr[:i], arr[i+1:]...)
			}
		}
		return arr
	}
	// todo: optimize, for now we copy whole slice for each table
	for _, table := range tables {
		if l == 0 {
			env.l0 = remove(env.l0, table.GetId())
		} else {
			env.levels[l-1] = remove(env.levels[l-1], table.GetId())
		}
	}
}

// requires to hold db lock
func (env *dbEnv) applyEnv(db *db) error {
	db.manifest.SeqNum = env.seqNum.Load()
	db.manifest.LastFlushedFileNumber = env.lastFlushedFileNum.Load()
	db.manifest.L0 = env.l0
	db.manifest.Levels = env.levels
	db.manifest.NextFileNumber = env.nextFileNumber.Load()
	return db.manifest.fsync()
}
