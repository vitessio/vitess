/*
Copyright 2019 The Vitess Authors.

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

package logutil

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/log"
)

// ThrottledLogger will allow logging of messages but won't spam the
// logs.
//
// Info, Warn, and Error take a constant message and slog attributes, and
// record the logger's name in a "logger" attribute. Infof, Warningf, and
// Errorf format the message and prefix it with the logger's name. New code
// should use Info, Warn, and Error.
type ThrottledLogger struct {
	// set at construction
	name        string
	maxInterval time.Duration

	// mu protects the following members
	mu           sync.Mutex
	lastlogTime  time.Time
	skippedCount int
}

// NewThrottledLogger will create a ThrottledLogger with the given
// name and throttling interval.
func NewThrottledLogger(name string, maxInterval time.Duration) *ThrottledLogger {
	return &ThrottledLogger{
		name:        name,
		maxInterval: maxInterval,
	}
}

type logFunc func(int, string, ...slog.Attr)

var (
	infoDepth  = log.InfoDepth
	warnDepth  = log.WarnDepth
	errorDepth = log.ErrorDepth
)

// GetLastLogTime gets the last log time for the throttled logger.
func (tl *ThrottledLogger) GetLastLogTime() time.Time {
	return tl.lastlogTime
}

// throttle records a log attempt. It reports whether the caller should log
// now. When the attempt is the first one skipped in the current interval,
// report is true and wait is how long to wait before reporting the skipped
// count with reportSkipped.
func (tl *ThrottledLogger) throttle(now time.Time) (logNow, report bool, wait time.Duration) {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	wait = tl.maxInterval - now.Sub(tl.lastlogTime)
	if wait < 0 {
		tl.lastlogTime = now
		return true, false, 0
	}

	report = tl.skippedCount == 0
	tl.skippedCount++
	return false, report, wait
}

// reportSkipped waits, then resets the skipped count and logs it. It runs in
// its own goroutine, so the stack trace is lost and the report is logged at
// depth 0.
func (tl *ThrottledLogger) reportSkipped(wait time.Duration, report func(skipped int)) {
	<-time.After(wait)

	tl.mu.Lock()
	skipped := tl.skippedCount
	tl.skippedCount = 0
	tl.mu.Unlock()

	report(skipped)
}

func (tl *ThrottledLogger) log(logF logFunc, format string, v ...any) {
	logNow, report, wait := tl.throttle(time.Now())
	if logNow {
		logF(2, fmt.Sprintf(tl.name+": "+format, v...))
		return
	}
	if report {
		go tl.reportSkipped(wait, func(skipped int) {
			logF(0, fmt.Sprintf("%v: skipped %v log messages", tl.name, skipped))
		})
	}
}

func (tl *ThrottledLogger) logAttrs(logF logFunc, msg string, attrs []slog.Attr) {
	logNow, report, wait := tl.throttle(time.Now())
	if logNow {
		all := make([]slog.Attr, 0, len(attrs)+1)
		all = append(all, slog.String("logger", tl.name))
		all = append(all, attrs...)
		logF(2, msg, all...)
		return
	}
	if report {
		go tl.reportSkipped(wait, func(skipped int) {
			logF(0, "skipped log messages", slog.String("logger", tl.name), slog.Int("skipped", skipped))
		})
	}
}

// Info logs msg and attrs at INFO level if not throttled.
func (tl *ThrottledLogger) Info(msg string, attrs ...slog.Attr) {
	tl.logAttrs(infoDepth, msg, attrs)
}

// Warn logs msg and attrs at WARN level if not throttled.
func (tl *ThrottledLogger) Warn(msg string, attrs ...slog.Attr) {
	tl.logAttrs(warnDepth, msg, attrs)
}

// Error logs msg and attrs at ERROR level if not throttled.
func (tl *ThrottledLogger) Error(msg string, attrs ...slog.Attr) {
	tl.logAttrs(errorDepth, msg, attrs)
}

// Infof formats and logs an info if not throttled.
func (tl *ThrottledLogger) Infof(format string, v ...any) {
	tl.log(infoDepth, format, v...)
}

// Warningf formats and logs a warning if not throttled.
func (tl *ThrottledLogger) Warningf(format string, v ...any) {
	tl.log(warnDepth, format, v...)
}

// Errorf formats and logs an error if not throttled.
func (tl *ThrottledLogger) Errorf(format string, v ...any) {
	tl.log(errorDepth, format, v...)
}
