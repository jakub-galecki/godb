package log

import (
	"io"
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
func NewLogger(name string, dst io.WriteCloser) *Logger {
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
