package main

import (
	"bytes"
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
		}
	}
	return nil, false
}

func (l *level) AddMemtable(d *db, mem *memtable.MemTable) error {
	var (
		table *sst.SST
		err   error
	)

	if table, err = sst.WriteMemTable(mem, d.opts.path, l.blockCache, l.getNextSSTId()); err != nil {
		return err
	}
	l.ssts = append(l.ssts, table)
	d.manifest.addSst(l.id, table.GetId())
	l.curId++
	return nil
}

func (l *level) getNextSSTId() string {
	var buf bytes.Buffer
	// todo: hash
	buf.WriteString(strconv.Itoa(l.id))
	buf.WriteRune('.')
	buf.WriteString(strconv.Itoa(l.curId))
	return buf.String()
}

func (l *level) loadSSTs(ssts []string) error {
	getFile := func(name string) string {
		return path.Join(l.dir, name)
	}

	for _, ssFile := range ssts {
		p := getFile(ssFile)
		ss, err := sst.Open(p)
		if err != nil {
			return err
		}
		l.ssts = append(l.ssts, ss)
		l.curId++
	}

	return nil
}
