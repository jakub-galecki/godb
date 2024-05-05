package log

import (
	"fmt"
	"godb/common"
	"os"

	"github.com/rs/zerolog"
)

type Logger struct {
	zerolog.Logger
}

func NewLogger(name string) *Logger {
	if err := common.EnsureDir("./logs"); err != nil {
		panic(err)
	}

	f, err := os.OpenFile(fmt.Sprintf("./logs/%s.log", name), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	l := zerolog.New(f).With().Timestamp().Logger()
	return &Logger{l}
}
