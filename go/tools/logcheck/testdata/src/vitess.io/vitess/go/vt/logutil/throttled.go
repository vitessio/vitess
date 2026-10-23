/*
Copyright 2026 The Vitess Authors.

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
	"log/slog"
	"time"
)

type ThrottledLogger struct{}

func NewThrottledLogger(name string, maxInterval time.Duration) *ThrottledLogger {
	return &ThrottledLogger{}
}

func (tl *ThrottledLogger) Info(msg string, attrs ...slog.Attr) {}

func (tl *ThrottledLogger) Warn(msg string, attrs ...slog.Attr) {}

func (tl *ThrottledLogger) Error(msg string, attrs ...slog.Attr) {}

func (tl *ThrottledLogger) Infof(format string, v ...any) {}

func (tl *ThrottledLogger) Warningf(format string, v ...any) {}

func (tl *ThrottledLogger) Errorf(format string, v ...any) {}

// Logger has printf-style methods with the same names as ThrottledLogger's.
type Logger interface {
	Infof(format string, v ...any)
	Errorf(format string, v ...any)
}

// NewPrefixedLogger is not in the real package. Only go/vt/log's has its
// prefix checked.
func NewPrefixedLogger(prefix string) *ThrottledLogger { return &ThrottledLogger{} }

// reassigned mirrors the real package's use of its own methods. The analyzer
// skips this package, so none of these are findings.
func reassigned(tl *ThrottledLogger, msg string) {
	info := tl.Info
	info(msg)
	tl.Infof(msg)
}
