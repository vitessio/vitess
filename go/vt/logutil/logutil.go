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

// package logutil provides some utilities for logging using glog and
// redirects the stdlib logging to glog.

package logutil

import (
	stdlog "log"
	"log/slog"
	"strings"

	"vitess.io/vitess/go/vt/log"
)

// stdlogPrefix marks lines written through the standard library's log
// package. The shim logs those lines with the constant message
// "standard library log" and the line, without the prefix, under "message".
const stdlogPrefix = "log: "

type logShim struct{}

func (shim *logShim) Write(buf []byte) (n int, err error) {
	line := strings.TrimSuffix(strings.TrimPrefix(string(buf), stdlogPrefix), "\n")
	log.Info("standard library log", slog.String("message", line))
	return len(buf), nil
}

func init() {
	stdlog.SetPrefix(stdlogPrefix)
	stdlog.SetFlags(0)
	stdlog.SetOutput(new(logShim))
}
