/*
   Copyright 2014 Outbrain Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package log

import (
	"errors"
	"fmt"
	"log/syslog"
	"os"
	"runtime/debug"
	"time"
)

// LogLevel indicates the severity of a log entry
type LogLevel int

func (this LogLevel) String() string {
	switch this {
	case FATAL:
		return "FATAL"
	case CRITICAL:
		return "CRITICAL"
	case ERROR:
		return "ERROR"
	case WARNING:
		return "WARNING"
	case NOTICE:
		return "NOTICE"
	case INFO:
		return "INFO"
	case DEBUG:
		return "DEBUG"
	}
	return "unknown"
}

func LogLevelFromString(logLevelName string) (LogLevel, error) {
	switch logLevelName {
	case "FATAL":
		return FATAL, nil
	case "CRITICAL":
		return CRITICAL, nil
	case "ERROR":
		return ERROR, nil
	case "WARNING":
		return WARNING, nil
	case "NOTICE":
		return NOTICE, nil
	case "INFO":
		return INFO, nil
	case "DEBUG":
		return DEBUG, nil
	}
	return 0, fmt.Errorf("Unknown LogLevel name: %+v", logLevelName)
}

const (
	FATAL LogLevel = iota
	CRITICAL
	ERROR
	WARNING
	NOTICE
	INFO
	DEBUG
)

const TimeFormat = "2006-01-02 15:04:05"

// globalLogLevel indicates the global level filter for all logs (only entries with level equals or higher
// than this value will be logged)
var globalLogLevel LogLevel = DEBUG
var printStackTrace bool = false

// syslogWriter is optional, and defaults to nil (disabled)
var syslogLevel LogLevel = ERROR
var syslogWriter *syslog.Writer

// SetPrintStackTrace enables/disables dumping the stack upon error logging
func SetPrintStackTrace(shouldPrintStackTrace bool) {
	printStackTrace = shouldPrintStackTrace
}

// SetLevel sets the global log level. Only entries with level equals or higher than
// this value will be logged
func SetLevel(logLevel LogLevel) {
	globalLogLevel = logLevel
}

// GetLevel returns current global log level
func GetLevel() LogLevel {
	return globalLogLevel
}

// EnableSyslogWriter enables, if possible, writes to syslog. These will execute _in addition_ to normal logging
func EnableSyslogWriter(tag string) (err error) {
	syslogWriter, err = syslog.New(syslog.LOG_ERR, tag)
	if err != nil {
		syslogWriter = nil
	}
	return err
}

// SetSyslogLevel sets the minimal syslog level. Only entries with level equals or higher than
// this value will be logged. However, this is also capped by the global log level. That is,
// messages with lower level than global-log-level will be discarded at any case.
func SetSyslogLevel(logLevel LogLevel) {
	syslogLevel = logLevel
}

// logFormattedEntry nicely formats and emits a log entry
func logFormattedEntry(logLevel LogLevel, message string, args ...interface{}) string {
	if logLevel > globalLogLevel {
		return ""
	}
	// if TZ env variable is set, update the timestamp timezone
	localizedTime := time.Now()
	tzLocation := os.Getenv("TZ")
	if tzLocation != "" {
		location, err := time.LoadLocation(tzLocation)
		if err == nil { // if invalid tz location was provided, just leave it as the default
			localizedTime = time.Now().In(location)
		}
	}

	msgArgs := fmt.Sprintf(message, args...)
	entryString := fmt.Sprintf("%s %s %s", localizedTime.Format(TimeFormat), logLevel, msgArgs)
	fmt.Fprintln(os.Stderr, entryString)

	if syslogWriter != nil {
		go func() error {
			if logLevel > syslogLevel {
				return nil
			}
			switch logLevel {
			case FATAL:
				return syslogWriter.Emerg(msgArgs)
			case CRITICAL:
				return syslogWriter.Crit(msgArgs)
			case ERROR:
				return syslogWriter.Err(msgArgs)
			case WARNING:
				return syslogWriter.Warning(msgArgs)
			case NOTICE:
				return syslogWriter.Notice(msgArgs)
			case INFO:
				return syslogWriter.Info(msgArgs)
			case DEBUG:
				return syslogWriter.Debug(msgArgs)
			}
			return nil
		}()
	}
	return entryString
}

// logEntry emits a formatted log entry
func logEntry(logLevel LogLevel, message string, args ...interface{}) string {
	entryString := message
	for _, s := range args {
		entryString += fmt.Sprintf(" %s", s)
	}
	return logFormattedEntry(logLevel, entryString)
}

// logErrorEntry emits a log entry based on given error object
func logErrorEntry(logLevel LogLevel, err error) error {
	if err == nil {
		// No error
		return nil
	}
	entryString := fmt.Sprintf("%+v", err)
	logEntry(logLevel, entryString)
	if printStackTrace {
		debug.PrintStack()
	}
	return err
}

func Debug(message string, args ...interface{}) string {
	return logEntry(DEBUG, message, args...)
}

func Debugf(message string, args ...interface{}) string {
	return logFormattedEntry(DEBUG, message, args...)
}

func Info(message string, args ...interface{}) string {
	return logEntry(INFO, message, args...)
}

func Infof(message string, args ...interface{}) string {
	return logFormattedEntry(INFO, message, args...)
}

func Notice(message string, args ...interface{}) string {
	return logEntry(NOTICE, message, args...)
}

func Noticef(message string, args ...interface{}) string {
	return logFormattedEntry(NOTICE, message, args...)
}

func Warning(message string, args ...interface{}) error {
	return errors.New(logEntry(WARNING, message, args...))
}

func Warningf(message string, args ...interface{}) error {
	return errors.New(logFormattedEntry(WARNING, message, args...))
}

func Error(message string, args ...interface{}) error {
	return errors.New(logEntry(ERROR, message, args...))
}

func Errorf(message string, args ...interface{}) error {
	return errors.New(logFormattedEntry(ERROR, message, args...))
}

func Errore(err error) error {
	return logErrorEntry(ERROR, err)
}

func Critical(message string, args ...interface{}) error {
	return errors.New(logEntry(CRITICAL, message, args...))
}

func Criticalf(message string, args ...interface{}) error {
	return errors.New(logFormattedEntry(CRITICAL, message, args...))
}

func Criticale(err error) error {
	return logErrorEntry(CRITICAL, err)
}

// Fatal emits a FATAL level entry and exists the program
func Fatal(message string, args ...interface{}) error {
	logEntry(FATAL, message, args...)
	os.Exit(1)
	return errors.New(logEntry(CRITICAL, message, args...))
}

// Fatalf emits a FATAL level entry and exists the program
func Fatalf(message string, args ...interface{}) error {
	logFormattedEntry(FATAL, message, args...)
	os.Exit(1)
	return errors.New(logFormattedEntry(CRITICAL, message, args...))
}

// Fatale emits a FATAL level entry and exists the program
func Fatale(err error) error {
	logErrorEntry(FATAL, err)
	os.Exit(1)
	return err
}
