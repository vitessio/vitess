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

package funcvalues

import (
	"log/slog"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
)

func alias(name string) {
	logMethod := log.Error // want "logging function must be called directly, not used as a value"
	logMethod(name)
	depth := log.ErrorDepth // want "logging function must be called directly, not used as a value"
	depth(1, name)
}

func sink(fn func(msg string, attrs ...slog.Attr)) { fn("ok") }

func callback() {
	sink(log.Info) // want "logging function must be called directly, not used as a value"
}

func methodValue(name string) {
	pl := log.NewPrefixedLogger("p")
	warn := pl.Warn // want "logging function must be called directly, not used as a value"
	warn(name)

	tl := logutil.NewThrottledLogger("t", time.Second)
	errFn := tl.Error // want "logging function must be called directly, not used as a value"
	errFn(name)
	infof := tl.Infof // want "ThrottledLogger.Infof formats values into the message; log a constant message with slog attributes"
	infof("%v", name)

	v := log.V(1)
	info := v.Info // want "logging function must be called directly, not used as a value"
	info("ok")
}

func handler(msg string, attrs ...slog.Attr) {}

func assignAndCompare() {
	log.Info = handler   // want "logging function must be called directly, not used as a value"
	if log.Warn != nil { // want "logging function must be called directly, not used as a value"
		return
	}
	log.DefaultHooks.OnError = handler // want "logging function must be called directly, not used as a value"
	_ = log.Hooks{OnError: handler}
}

func constructor(name string) {
	mk := log.NewPrefixedLogger // want "logging function must be called directly, not used as a value"
	mk(name).Info("ok")
}

// V and Flush are not logging functions.
func notLogging() {
	vf := log.V
	_ = vf(1)
	flush := log.Flush
	flush()
}
