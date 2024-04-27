package wal

import (
	"container/list"
	"errors"
	"fmt"
	"godb/common"
	"path"
	"sync"
)

type Manager struct {
	mu sync.Mutex

	cur  *writer
	wals *list.List
	opts *Opts
}

func Init(o *Opts) (*Manager, error) {
	if o == nil || o.Dir == "" {
		return nil, errors.New("invalid options")
	}

	m := &Manager{
		opts: o,
		wals: list.New(),
	}

	if err := common.EnsureDir(m.opts.Dir); err != nil {
		return nil, fmt.Errorf("ensuring wal dir %w", err)
	}

	return m, nil
}

func (m *Manager) NewWAL(logNum WalLogNum) (*writer, error) {
	fname := fmt.Sprintf("%s.log", logNum.String())
	f, err := common.CreateFile(path.Join(m.opts.Dir, fname))
	if err != nil {
		return nil, err
	}

	m.cur, err = newWriter(f, m.opts)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	m.wals.PushBack(logNum)
	m.mu.Unlock()

	return m.cur, nil
}
