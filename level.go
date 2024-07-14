package main

import (
	"errors"
	"godb/common"
	"godb/internal/cache"
	"godb/memtable"
	"godb/sst"
	"path"
	"strconv"
)

type level struct {
	id int
	//min, max []byte
	ssts       []*sst.SST
	blockCache *cache.Cache[[]byte]
	dir        string
	curId      int
}

func newLevel(id int, dir string, cache *cache.Cache[[]byte]) *level {
	lvl := level{
		id:         id,
		blockCache: cache,
		dir:        dir,
		curId:      0,
	}
	return &lvl
}

func (l *level) Get(key []byte) ([]byte, bool) {
	for _, tbl := range l.ssts {
		if value, err := tbl.Get(key); err == nil {
			return value, true
		} else {
			if errors.Is(err, sst.NOT_FOUND_IN_BLOOM) {
				continue
			}
			// trace.Error().Str("sstId", tbl.GetId()).Err(err).Msg("error while getting data from sst")
		}
	}
	return nil, false
}

func (l *level) AddMemtable(d *db, mem *memtable.MemTable) (*sst.SST, error) {
	var (
		table *sst.SST
		err   error
	)

	if table, err = sst.WriteMemTable(d.logger, mem, path.Join(d.opts.path, common.SST_DIR), l.blockCache,
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
	getFile := func(name string) string {
		return path.Join(l.dir, name)
	}

	for _, ssId := range ssts {
		p := getFile(ssId)
		ss, err := sst.Open(p, ssId)
		if err != nil {
			return err
		}
		l.ssts = append(l.ssts, ss)
		l.curId++
	}

	return nil
}
