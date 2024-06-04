package log

import (
	"fmt"
	"godb/common"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type Logger struct {
	*zerolog.Logger
	f *os.File
}

var loggerPool = sync.Pool{
	New: func() any {
		return new(zerolog.Logger)
	},
}

func NewLogger(name string) *Logger {
	logPath := os.Getenv("GODB_LOG_PATH")
	if logPath == "" {
		logPath = "/tmp/logs"
	}
	if err := common.EnsureDir(logPath); err != nil {
		panic(err)
	}
	f, err := os.OpenFile(fmt.Sprintf("./logs/%s.log", name), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	l := loggerPool.Get().(*zerolog.Logger)
	*l = l.Output(f).With().Timestamp().Logger()
	return &Logger{Logger: l, f: f}
}

func (l *Logger) Event(name string, start time.Time) {
	l.Logger.Info().Str("method", "name").Dur("elapsed", time.Since(start))
}

func (l *Logger) Release() {
	loggerPool.Put(l.Logger)
	if err := l.f.Close(); err != nil {
		panic(err)
	}
}

func (l *Logger) NewRequest()
