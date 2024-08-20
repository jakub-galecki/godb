package log

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jakub-galecki/godb/common"

	"github.com/rs/zerolog"
)

type LoggerType uint8

const (
	JsonLogger LoggerType = iota
	NilLogger
)

type discardWriterCloser struct {
	io.Writer
}

func (discardWriterCloser) Close() error {
	return nil
}

type Logger struct {
	zerolog.Logger
	f io.WriteCloser
}

func NewLogger(name string, t LoggerType) *Logger {
	jsonLogger := func() *Logger {
		var (
			dst io.WriteCloser = os.Stdout
			err error

			logPath = os.Getenv("GODB_LOG_PATH")
		)
		if logPath != "" {
			if err := common.EnsureDir(logPath); err != nil {
				panic(err)
			}
			dst, err = os.OpenFile(fmt.Sprintf("%s/%s.log", logPath, name), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				panic(err)
			}
		}
		return &Logger{Logger: zerolog.New(dst).With().Timestamp().Logger(), f: dst}
	}

	switch t {
	case JsonLogger:
		return jsonLogger()
	case NilLogger:
		dst := discardWriterCloser{Writer: io.Discard}
		return &Logger{Logger: zerolog.New(dst).With().Timestamp().Logger(), f: dst}
	default:
		panic("unknown logger type")
	}
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
