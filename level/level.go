package level

import (
	"github.com/allegro/bigcache"
	"go.uber.org/zap"

	"godb/log"
	"godb/memtable"
	"godb/sst"
)

type Level interface {
	Get(key []byte) ([]byte, bool)
	AddMemtable(mem *memtable.MemTable) error
}

type level struct {
	id     int
	table  string
	logger *zap.SugaredLogger
	//min, max []byte
	ssts       []*sst.SST
	blockCache *bigcache.BigCache
	curId      int
}

func NewLevel(id int, table string, cache *bigcache.BigCache) Level {
	lvl := level{
		id:         id,
		table:      table,
		logger:     log.InitLogger(),
		blockCache: cache,
	}

	lvl.loadSSTs()
	return &lvl
}

func (l *level) Get(key []byte) ([]byte, bool) {
	for _, tbl := range l.ssts {
		if value, err := tbl.Get(key); err == nil {
			return value, true
		}
	}
	return nil, false
}

func (l *level) AddMemtable(mem *memtable.MemTable) error {
	var (
		table *sst.SST
		err   error
	)

	if table, err = sst.WriteMemTable(mem, l.table, l.blockCache, l.curId, l.id); err != nil {
		return err
	}
	l.ssts = append(l.ssts, table)
	l.curId++
	return nil
}

func (l *level) loadSSTs() {
	// todo
}
