// SPDX-FileCopyrightText: 2019 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package log

import (
	"fmt"
	"runtime"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

var logger = New()

// Logger struct is ...
type Logger struct {
	sync.RWMutex
	entry *logrus.Entry
	log   *logrus.Logger
}

// New creates new Logger instance
func New() (log *Logger) {
	logger := logrus.New()
	return &Logger{
		log:   logger,
		entry: logrus.NewEntry(logger),
	}
}

// WithFields adds additional log fields
func WithFields(fields logrus.Fields) *logrus.Entry {
	entry := logger.entry.WithFields(fields)
	if logger.log.Level >= logrus.DebugLevel {
		entry.Data["file"] = fileInfo(2)
	}
	return entry
}

// Fields wraps logrus.Fields, which is a map[string]interface{}
type Fields logrus.Fields

// SetLevel sets the standard logger level.
func SetLevel(level logrus.Level) {
	fmt.Println(level.String())
	logger.log.Level = level
}

// SetFormatter sets the standard logger formatter.
func SetFormatter(formatter logrus.Formatter) {
	logger.log.Formatter = formatter
}

// Debug logs a message at level Debug on the standard logger.
func Debug(args ...any) {
	if logger.log.Level >= logrus.DebugLevel {
		logger.Lock()
		defer logger.Unlock()
		logger.entry.Data["file"] = fileInfo(2)
		logger.entry.Debug(args...)
	}
}

// Info logs a message at level Info on the standard logger.
func Info(args ...any) {
	if logger.log.Level >= logrus.InfoLevel {
		logger.Lock()
		defer logger.Unlock()
		logger.entry.Data["file"] = fileInfo(2)
		logger.entry.Info(args...)
	}
}

// Warn logs a message at level Warn on the standard logger.
func Warn(args ...any) {
	if logger.log.Level >= logrus.WarnLevel {
		logger.Lock()
		defer logger.Unlock()
		logger.entry.Data["file"] = fileInfo(2)
		logger.entry.Warn(args...)
	}
}

// Error logs a message at level Error on the standard logger.
func Error(args ...any) {
	if logger.log.Level >= logrus.ErrorLevel {
		logger.Lock()
		defer logger.Unlock()
		logger.entry.Data["file"] = fileInfo(2)
		logger.entry.Error(args...)
	}
}

// Fatal logs a message at level Fatal on the standard logger.
func Fatal(args ...any) {
	if logger.log.Level >= logrus.FatalLevel {
		logger.Lock()
		defer logger.Unlock()
		logger.entry.Data["file"] = fileInfo(2)
		logger.entry.Fatal(args...)
	}
}

// Panic logs a message at level Panic on the standard logger.
func Panic(args ...any) {
	if logger.log.Level >= logrus.PanicLevel {
		logger.Lock()
		defer logger.Unlock()
		logger.entry.Data["file"] = fileInfo(2)
		logger.entry.Panic(args...)
	}
}

func fileInfo(skip int) string {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		file = "<???>"
		line = 1
	} else {
		slash := strings.LastIndex(file, "/")
		if slash >= 0 {
			file = file[slash+1:]
		}
	}
	return fmt.Sprintf("%s:%d", file, line)
}
