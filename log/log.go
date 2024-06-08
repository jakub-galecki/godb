package log

import (
	"fmt"
	"godb/common"
	"io"
	"os"
	"time"

	"github.com/rogpeppe/fastuuid"
	"github.com/rs/zerolog"
)

type Logger struct {
	*zerolog.Logger
	f       io.WriteCloser
	uuidGen *fastuuid.Generator
}

// var loggerPool = sync.Pool{
// 	New: func() any {
// 		return new(zerolog.Logger)
// 	},
// }

func NewLogger(name string) *Logger {
	logPath := os.Getenv("GODB_LOG_PATH")
	var dst io.WriteCloser
	var err error
	if logPath != "" {
		if err := common.EnsureDir(logPath); err != nil {
			panic(err)
		}
		dst, err = os.OpenFile(fmt.Sprintf("%s/%s.log", logPath, name), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
	} else {
		dst = os.Stdout
	}
	l := zerolog.New(dst).With().Timestamp().Logger()
	return &Logger{Logger: &l, f: dst, uuidGen: fastuuid.MustNewGenerator()}
}

func (l *Logger) WithId() *Logger {
	lc := l.Logger.With().Logger().Output(l.f)
	id := l.uuidGen.Next()
	lc.UpdateContext(func(c zerolog.Context) zerolog.Context {
		return c.Str("id", string(id[:]))
	})
	return &Logger{Logger: &lc}
}

func (l *Logger) Event(name string, start time.Time) {
	l.Logger.Info().Str("method", "name").Dur("elapsed", time.Since(start))
}

func (l *Logger) Release() {
	if err := l.f.Close(); err != nil {
		panic(err)
	}
}
