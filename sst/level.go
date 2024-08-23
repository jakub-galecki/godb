package sst

import (
	"errors"
	"strconv"

	"github.com/jakub-galecki/godb/internal/cache"
	"github.com/jakub-galecki/godb/log"
	"github.com/jakub-galecki/godb/memtable"
)

type Level struct {
	id int
	//min, max []byte
	ssts       []*SST
	blockCache cache.Cacher[[]byte]
	dir        string
	curId      int
	logger     *log.Logger
}

func NewLevel(id int, dir string, cache cache.Cacher[[]byte], logger *log.Logger) *Level {
	lvl := Level{
		id:         id,
		blockCache: cache,
		dir:        dir,
		curId:      0,
		logger:     logger,
	}
	return &lvl
}

func (l *Level) Get(key []byte) ([]byte, bool) {
	for _, tbl := range l.ssts {
		val, err := tbl.Get(key)
		if err != nil {
			if errors.Is(err, ErrNotFoundInBloom) {
				continue
			}
			l.logger.Error().Str("sstId", tbl.GetId()).Err(err).Msg("error while getting data from sst")
			return nil, false
		}
		return val, true
	}
	return nil, false
}

func (l *Level) AddMemtable(mem *memtable.MemTable) (*SST, error) {
	var (
		table *SST
		err   error
	)

	if table, err = WriteMemTable(l.logger, mem, l.dir, l.blockCache,
		strconv.FormatUint(mem.GetFileNum(), 10)); err != nil {
		return nil, err
	}
	l.ssts = append(l.ssts, table)
	l.curId++
	return table, nil
}

func (l *Level) GetTables() []*SST {
	return l.ssts
}

func (l *Level) LoadTables(ssts []string) error {
	for _, ssId := range ssts {
		ss, err := Open(l.dir, ssId, l.logger)
		if err != nil {
			return err
		}
		l.ssts = append(l.ssts, ss)
		l.curId++
	}
	return nil
}

func (l *Level) GetDir() string {
	return l.dir
}

func (l *Level) GetId() int {
	return l.id
}

func (l *Level) GetOldest() *SST {
	return l.ssts[len(l.ssts)-1]
}
