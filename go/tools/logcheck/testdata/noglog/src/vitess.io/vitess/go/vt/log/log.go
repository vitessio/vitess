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

package log

import "log/slog"

var (
	Debug = func(msg string, attrs ...slog.Attr) {}
	Info  = func(msg string, attrs ...slog.Attr) {}
	Warn  = func(msg string, attrs ...slog.Attr) {}
	Error = func(msg string, attrs ...slog.Attr) {}

	DebugDepth = func(depth int, msg string, attrs ...slog.Attr) {}
	InfoDepth  = func(depth int, msg string, attrs ...slog.Attr) {}
	WarnDepth  = func(depth int, msg string, attrs ...slog.Attr) {}
	ErrorDepth = func(depth int, msg string, attrs ...slog.Attr) {}
)

func Enabled(level slog.Level) bool { return true }

type PrefixedLogger struct {
	prefix string
}

func NewPrefixedLogger(prefix string) *PrefixedLogger {
	return &PrefixedLogger{prefix: prefix}
}

func (pl *PrefixedLogger) Info(msg string, attrs ...slog.Attr) {}

func (pl *PrefixedLogger) Warn(msg string, attrs ...slog.Attr) {}

func (pl *PrefixedLogger) Error(msg string, attrs ...slog.Attr) {}

func (pl *PrefixedLogger) InfoDepth(depth int, msg string, attrs ...slog.Attr) {}

func (pl *PrefixedLogger) WarnDepth(depth int, msg string, attrs ...slog.Attr) {}

func (pl *PrefixedLogger) ErrorDepth(depth int, msg string, attrs ...slog.Attr) {}
