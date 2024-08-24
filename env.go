package godb

import (
	"github.com/jakub-galecki/godb/sst"
	"slices"
	"sync/atomic"
)

type dbEnv struct {
	seqNum             atomic.Uint64
	lastFlushedFileNum atomic.Uint64
	l0                 []string
	levels             [][]string
}

func envFromManifest(m *Manifest) *dbEnv {
	env := &dbEnv{}
	env.seqNum.Store(m.SeqNum)
	env.lastFlushedFileNum.Store(m.LastFlushedFileNumber)
	env.l0 = make([]string, 0)
	env.levels = make([][]string, m.MaxLevels)
	return env
}

func (env *dbEnv) refresh(m *Manifest) {
	env.seqNum.Store(m.SeqNum)
	env.lastFlushedFileNum.Store(m.LastFlushedFileNumber)
	env.l0 = m.L0
	env.levels = m.Levels
}

func (env *dbEnv) getSeqNum(count int) uint64 {
	seq := env.seqNum.Add(uint64(count)) - uint64(count) + 1
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
			env.levels[l] = append(env.levels[l], table.GetId())
		}
	}
}

// requires to hold db lock
func (env *dbEnv) remove(l int, tables ...*sst.SST) {
	remove := func(arr []string, id string) []string {
		i, found := slices.BinarySearch(arr, id)
		if !found {
			return arr
		}
		return append(arr[:i], arr[i+1:]...)
	}
	// todo: optimize, for now we copy whole slice for each table
	for _, table := range tables {
		if l == 0 {
			env.l0 = remove(env.l0, table.GetId())
		} else {
			env.levels[l] = remove(env.levels[l], table.GetId())
		}
	}
}

// requires to hold db lock
func (env *dbEnv) applyEnv(db *db) error {
	db.manifest.SeqNum = env.seqNum.Load()
	db.manifest.LastFlushedFileNumber = env.lastFlushedFileNum.Load()
	db.manifest.L0 = env.l0
	db.manifest.Levels = env.levels
	return db.manifest.fsync()
}
