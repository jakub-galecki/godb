package log

import (
	"fmt"
	"godb/common"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

type Logger struct {
	zerolog.Logger
	f io.WriteCloser
}

//	var loggerPool = sync.Pool{
//		New: func() any {
//			return new(zerolog.Logger)
//		},
//	}
//

// todo: add  options with log level
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
	return &Logger{Logger: zerolog.New(dst).With().Timestamp().Logger(), f: dst}
}

func (l *Logger) Event(name string, start time.Time) {
	dur := time.Since(start)
	if dur == 0 {
		return
	}
	l.Logger.Info().Str("method", name).Dur("elapsed", dur).Send()
}

func (l *Logger) Release() {
	if err := l.f.Close(); err != nil {
		panic(err)
	}
}
