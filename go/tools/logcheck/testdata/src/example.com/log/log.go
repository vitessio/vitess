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

// This package mirrors the names and signatures of go/vt/log under another
// import path.

var (
	Info  = func(msg string, attrs ...slog.Attr) {}
	Error = func(msg string, attrs ...slog.Attr) {}
)

type PrefixedLogger struct{}

func NewPrefixedLogger(prefix string) *PrefixedLogger { return &PrefixedLogger{} }

func (pl *PrefixedLogger) Warn(msg string, attrs ...slog.Attr) {}
