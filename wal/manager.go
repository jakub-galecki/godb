package wal

import (
	"container/list"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/jakub-galecki/godb/common"
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
	f, err := common.CreateFile(path.Join(m.opts.Dir, logNum.FileName()))
	if err != nil {
		return nil, err
	}

	if m.cur != nil {
		err = m.cur.Close()
		if err != nil {
			return nil, err
		}
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

func (m *Manager) OpenWAL(logNum WalLogNum) (*writer, error) {
	f, err := os.OpenFile(path.Join(m.opts.Dir, logNum.FileName()), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
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
