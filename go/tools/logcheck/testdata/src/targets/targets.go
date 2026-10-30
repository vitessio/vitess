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

package targets

import (
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
)

func levels(name string) {
	log.Debug("ok")
	log.Debug(name) // want "log message must be a constant; pass values as slog attributes"
	log.Info("ok")
	log.Info(name) // want "log message must be a constant; pass values as slog attributes"
	log.Warn("ok")
	log.Warn(name) // want "log message must be a constant; pass values as slog attributes"
	log.Error("ok")
	log.Error(name) // want "log message must be a constant; pass values as slog attributes"
}

func depths(name string) {
	log.DebugDepth(1, "ok")
	log.DebugDepth(1, name) // want "log message must be a constant; pass values as slog attributes"
	log.InfoDepth(1, "ok")
	log.InfoDepth(1, name) // want "log message must be a constant; pass values as slog attributes"
	log.WarnDepth(1, "ok")
	log.WarnDepth(1, name) // want "log message must be a constant; pass values as slog attributes"
	log.ErrorDepth(1, "ok")
	log.ErrorDepth(1, name) // want "log message must be a constant; pass values as slog attributes"
}

func verbose(name string) {
	log.V(2).Info("ok")
	log.V(2).Info(name) // want "log message must be a constant; pass values as slog attributes"
}

func prefixed(name string) {
	pl := log.NewPrefixedLogger("component")
	pl.Info("ok")
	pl.Info(name) // want "log message must be a constant; pass values as slog attributes"
	pl.Warn("ok")
	pl.Warn(name) // want "log message must be a constant; pass values as slog attributes"
	pl.Error("ok")
	pl.Error(name) // want "log message must be a constant; pass values as slog attributes"
	pl.InfoDepth(1, "ok")
	pl.InfoDepth(1, name) // want "log message must be a constant; pass values as slog attributes"
	pl.WarnDepth(1, "ok")
	pl.WarnDepth(1, name) // want "log message must be a constant; pass values as slog attributes"
	pl.ErrorDepth(1, "ok")
	pl.ErrorDepth(1, name) // want "log message must be a constant; pass values as slog attributes"
	pl.V(1).Info(name)     // want "log message must be a constant; pass values as slog attributes"
}

func throttled(name string) {
	tl := logutil.NewThrottledLogger("component", time.Second)
	tl.Info("ok")
	tl.Info(name) // want "log message must be a constant; pass values as slog attributes"
	tl.Warn("ok")
	tl.Warn(name) // want "log message must be a constant; pass values as slog attributes"
	tl.Error("ok")
	tl.Error(name) // want "log message must be a constant; pass values as slog attributes"
}

func throttledPrintf(name string) {
	tl := logutil.NewThrottledLogger("component", time.Second)
	tl.Infof("ok")             // want "ThrottledLogger.Infof formats values into the message; log a constant message with slog attributes"
	tl.Warningf("%v", name)    // want "ThrottledLogger.Warningf formats values into the message; log a constant message with slog attributes"
	tl.Errorf("bad: %v", name) // want "ThrottledLogger.Errorf formats values into the message; log a constant message with slog attributes"
}

// logutil.Logger's printf-style methods are not checked.
func loggerInterface(l logutil.Logger, name string) {
	l.Infof("%v", name)
	l.Errorf(name)
}

func namedFuncType(name string) {
	log.Notice("ok")
	log.Notice(name) // want "log message must be a constant; pass values as slog attributes"
}

func aliasedAttr(name string) {
	log.InfoFields("ok")
	log.InfoFields(name) // want "log message must be a constant; pass values as slog attributes"
}

func promotedFromUnexported(name string) {
	l := log.NewLogger()
	l.Info("ok")
	l.Info(name) // want "log message must be a constant; pass values as slog attributes"
}

func funcField(name string) {
	log.DefaultHooks.OnError("ok")
	log.DefaultHooks.OnError(name) // want "log message must be a constant; pass values as slog attributes"
}
