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

import "fmt"

type PrefixedLogger struct {
	prefix string
}

func NewPrefixedLogger(prefix string) *PrefixedLogger {
	return &PrefixedLogger{prefix: prefix + ": "}
}

func (pl *PrefixedLogger) Flush() {
	Flush()
}

func (pl *PrefixedLogger) Info(args ...any) {
	args = append([]any{pl.prefix}, args...)
	Info(fmt.Sprint(args...))
}

func (pl *PrefixedLogger) Infof(format string, args ...any) {
	args = append([]any{pl.prefix}, args...)
	Info(fmt.Sprintf("%s"+format, args...))
}

func (pl *PrefixedLogger) InfoDepth(depth int, args ...any) {
	args = append([]any{pl.prefix}, args...)
	InfoDepth(depth, fmt.Sprint(args...))
}

func (pl *PrefixedLogger) Warning(args ...any) {
	args = append([]any{pl.prefix}, args...)
	Warn(fmt.Sprint(args...))
}

func (pl *PrefixedLogger) Warningf(format string, args ...any) {
	args = append([]any{pl.prefix}, args...)
	Warn(fmt.Sprintf("%s"+format, args...))
}

func (pl *PrefixedLogger) WarningDepth(depth int, args ...any) {
	args = append([]any{pl.prefix}, args...)
	WarnDepth(depth, fmt.Sprint(args...))
}

func (pl *PrefixedLogger) Error(args ...any) {
	args = append([]any{pl.prefix}, args...)
	Error(fmt.Sprint(args...))
}

func (pl *PrefixedLogger) Errorf(format string, args ...any) {
	args = append([]any{pl.prefix}, args...)
	Error(fmt.Sprintf("%s"+format, args...))
}

func (pl *PrefixedLogger) ErrorDepth(depth int, args ...any) {
	args = append([]any{pl.prefix}, args...)
	ErrorDepth(depth, fmt.Sprint(args...))
}
