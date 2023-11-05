package level

import (
	"go.uber.org/zap"

	"godb/log"
	"godb/memtable"
	"godb/sst"
)

type Level interface {
	Get(key []byte) ([]byte, bool)
	AddMemtable(mem memtable.MemTable) error
}

type level struct {
	id     uint
	table  string
	logger *zap.SugaredLogger
	//min, max []byte
	ssts []sst.SST
}

func NewLevel(id uint, table string) Level {
	lvl := level{
		id:     id,
		table:  table,
		logger: log.InitLogger(),
	}

	lvl.loadSSTs()
	return &lvl
}

func (l *level) Get(key []byte) ([]byte, bool) {
	for _, tbl := range l.ssts {
		if value, err := tbl.Get(key); err == nil {
			return value, true
		} else {
			l.logger.Errorf("[level::Get] error while getting: %v", err)
		}
	}
	return nil, false
}

func (l *level) AddMemtable(mem memtable.MemTable) error {
	var (
		table sst.SST
		err   error
	)

	if table, err = sst.WriteMemTable(mem, l.table); err != nil {
		return err
	}
	l.ssts = append(l.ssts, table)
	return nil
}

func (l *level) loadSSTs() {
	// todo
}
