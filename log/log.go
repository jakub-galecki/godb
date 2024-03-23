package log

import (
	"fmt"
	"os"

	"github.com/rs/zerolog"
)

type Logger struct {
	zerolog.Logger
}

func NewLogger(name string) *Logger {
	f, err := os.OpenFile(fmt.Sprintf("./log/logs/%s.log", name), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	l := zerolog.New(f).With().Timestamp().Logger()
	return &Logger{l}
}
