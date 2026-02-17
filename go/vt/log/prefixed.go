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

type PrefixedLogger struct {
	prefix string
}

func NewPrefixedLogger(prefix string) *PrefixedLogger {
	return &PrefixedLogger{prefix: prefix + ": "}
}

func (pl *PrefixedLogger) Info(msg string, attrs ...slog.Attr) { InfoDepth(1, pl.prefix+msg, attrs...) }
func (pl *PrefixedLogger) Warn(msg string, attrs ...slog.Attr) { WarnDepth(1, pl.prefix+msg, attrs...) }
func (pl *PrefixedLogger) Error(msg string, attrs ...slog.Attr) {
	ErrorDepth(1, pl.prefix+msg, attrs...)
}

func (pl *PrefixedLogger) InfoDepth(depth int, msg string, attrs ...slog.Attr) {
	InfoDepth(depth+1, pl.prefix+msg, attrs...)
}

func (pl *PrefixedLogger) WarnDepth(depth int, msg string, attrs ...slog.Attr) {
	WarnDepth(depth+1, pl.prefix+msg, attrs...)
}

func (pl *PrefixedLogger) ErrorDepth(depth int, msg string, attrs ...slog.Attr) {
	ErrorDepth(depth+1, pl.prefix+msg, attrs...)
}

func (pl *PrefixedLogger) V(level Level) Verbose { return V(level) }
func (pl *PrefixedLogger) Flush()                { Flush() }
