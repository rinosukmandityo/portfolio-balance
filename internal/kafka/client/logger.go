package client

import (
	"fmt"

	"go.uber.org/zap"
)

type errLogger struct {
	l *zap.Logger
}

func newErrorLogger(l *zap.Logger) *errLogger {
	return &errLogger{
		l: l,
	}
}

// Printf act like fmt.Printf but write to this errLogger instance.
func (l *errLogger) Printf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	l.l.Warn(msg)
}
