package wal

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"godb/common"
)

/*
NOTE
	For now this is the simple implementation with single log file without
	any segments.
*/

type WalOpts struct {
	Dir string

	SyncInterval time.Duration
	Encoder      func([]byte) []byte
	Sync         bool
	// TimeFormat   string
	// Logger *zap.Logger
	// todo: create Segment
}

func GetDefaultOpts(dir string) *WalOpts {
	return &WalOpts{
		Dir:          dir,
		SyncInterval: time.Second,
		Encoder:      func(b []byte) []byte { return b },
		Sync:         true,
		// TimeFormat:   "2001-12-01:",
	}
}

type Wal struct {
	path string

	mu struct {
		sync.Mutex

		file *os.File
		off  int64
		size int64
		// seqId int64
		lsn int
	}

	// atomic struct {
	// }

	syncInterval time.Duration
	syncTicker   *time.Ticker
	fsync        bool
	encd         func([]byte) []byte
	buf          *bufio.Writer
}

func NewWal(opts *WalOpts) (*Wal, error) {
	var (
		err error
	)

	if opts == nil {
		return nil, errors.New("empty options")
	}

	path := path.Join(opts.Dir, common.WAL)
	wl := &Wal{
		path:         path,
		syncInterval: opts.SyncInterval,
		syncTicker:   time.NewTicker(opts.SyncInterval),
		fsync:        opts.Sync,
		encd:         opts.Encoder,
	}

	wl.mu.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	wl.buf = bufio.NewWriter(wl.mu.file)

	go wl.runSyncWorker()

	return wl, nil
}

func (w *Wal) internalEncode(b []byte) []byte {
	lsn := w.mu.lsn
	data := []byte(fmt.Sprintf("%v %d %s\n", time.Now().Unix(), lsn, b))
	// sum := md5.Sum([]byte(data))
	return data
}

func (w *Wal) runSyncWorker() {
	for range w.syncTicker.C {
		w.mu.Lock()
		w.sync()
		w.mu.Unlock()
	}
}

// sync requiers to hold lock on mu
func (w *Wal) sync() error {
	return w.mu.file.Sync()
}

func (w *Wal) Write(data []byte, wg *sync.WaitGroup) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	enc := w.internalEncode(w.encd(data))
	if _, err := w.mu.file.Write(enc); err != nil {
		return err
	}

	w.mu.lsn += 1

	if wg != nil {
		wg.Done()
	}

	return nil
}

func (w *Wal) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.sync(); err != nil {
		return err
	}

	if err := w.mu.file.Close(); err != nil {
		return err
	}

	return nil
}

func (w *Wal) Delete() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.mu.file.Close(); err != nil {
		return err
	}

	if err := os.Remove(w.path); err != nil {
		return err
	}

	return nil
}
