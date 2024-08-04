package logger

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
)

var globalLogger zerolog.Logger

func init() {
	globalLogger = zerolog.New(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: "15:04:05.99",
		FormatMessage: func(i interface{}) string {
			return fmt.Sprintf("| %s |", i)
		},
		FormatCaller: func(i interface{}) string {
			return filepath.Base(fmt.Sprintf("%s", i))
		},
	}).With().
		Timestamp().
		Caller().
		Logger()
}

// GetLogger retrieves the global zerolog logger
func GetLogger() zerolog.Logger {
	return globalLogger
}
