package main

import (
	"errors"
	"godb/common"
	"godb/internal/cache"
	"godb/log"
	"godb/memtable"
	"godb/sst"
	"path"
	"strconv"
)

type level struct {
	id int
	//min, max []byte
	ssts       []*sst.SST
	blockCache cache.Cacher[[]byte]
	dir        string
	curId      int
	logger     *log.Logger
}

func newLevel(id int, dir string, cache cache.Cacher[[]byte], logger *log.Logger) *level {
	lvl := level{
		id:         id,
		blockCache: cache,
		dir:        dir,
		curId:      0,
		logger:     logger,
	}
	return &lvl
}

func (l *level) Get(key []byte) ([]byte, bool) {
	for _, tbl := range l.ssts {
		val, err := tbl.Get(key)
		if err != nil {
			if errors.Is(err, sst.ErrNotFoundInBloom) {
				continue
			}
			l.logger.Error().Str("sstId", tbl.GetId()).Err(err).Msg("error while getting data from sst")
			return nil, false
		}
		return val, true
	}
	return nil, false
}

func (l *level) AddMemtable(d *db, mem *memtable.MemTable) (*sst.SST, error) {
	var (
		table *sst.SST
		err   error
	)

	if table, err = sst.WriteMemTable(d.opts.logger, mem, path.Join(d.opts.path, common.SST_DIR), l.blockCache,
		strconv.FormatUint(mem.GetFileNum(), 10)); err != nil {
		return nil, err
	}
	l.ssts = append(l.ssts, table)
	l.curId++
	return table, nil
}

//func (l *level) getNextSSTId() string {
//	// todo: hash
//	return common.Concat(strconv.Itoa(l.id), ".", strconv.Itoa(l.curId))
//}

func (l *level) loadSSTs(ssts []string) error {
	for _, ssId := range ssts {
		ss, err := sst.Open(l.dir, ssId, l.logger)
		if err != nil {
			return err
		}
		l.ssts = append(l.ssts, ss)
		l.curId++
	}

	return nil
}
