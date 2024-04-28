package wal

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"
)

type writer struct {
	mu sync.Mutex

	o          *Opts
	file       *os.File
	off        int64
	size       int64
	lsn        int
	buf        *bufio.Writer
	syncTicker *time.Ticker
}

type Writer interface {
	Write([]byte, *sync.WaitGroup) error
	Close() error
}

func newWriter(f *os.File, o *Opts) (*writer, error) {
	w := &writer{
		file:       f,
		syncTicker: time.NewTicker(o.SyncInterval),
		o:          o,
	}

	w.buf = bufio.NewWriter(f)

	go w.runSyncWorker()
	return w, nil
}

func (w *writer) internalEncode(b []byte) []byte {
	lsn := w.lsn
	data := []byte(fmt.Sprintf("%v %d|%s\n", time.Now().Unix(), lsn, b))
	// sum := md5.Sum([]byte(data))
	return data
}

func (w *writer) runSyncWorker() {
	for range w.syncTicker.C {
		w.mu.Lock()
		w.sync()
		w.mu.Unlock()
	}
}

// sync requiers to hold lock on mu
func (w *writer) sync() error {
	return w.file.Sync()
}

func (w *writer) Write(data []byte, wg *sync.WaitGroup) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	enc := w.internalEncode(w.o.Encoder(data))
	if _, err := w.file.Write(enc); err != nil {
		return err
	}
	w.lsn += 1
	if wg != nil {
		wg.Done()
	}

	return nil
}

func (w *writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.sync(); err != nil {
		return err
	}

	if err := w.file.Close(); err != nil {
		return err
	}

	return nil
}
