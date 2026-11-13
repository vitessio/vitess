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

package calls

import (
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
)

type box struct {
	*log.PrefixedLogger
}

type throttledBox struct {
	*logutil.ThrottledLogger
}

func throttledPrintf(tl *logutil.ThrottledLogger, b throttledBox, name string) {
	(*logutil.ThrottledLogger).Errorf(tl, "%v", name) // want "ThrottledLogger.Errorf formats values into the message; log a constant message with slog attributes"
	b.Warningf("%v", name)                            // want "ThrottledLogger.Warningf formats values into the message; log a constant message with slog attributes"
}

func methodExpr(pl *log.PrefixedLogger, name string) {
	(*log.PrefixedLogger).Info(pl, "ok")
	(*log.PrefixedLogger).Info(pl, name) // want "log message must be a constant; pass values as slog attributes"
	(*log.PrefixedLogger).InfoDepth(pl, 1, "ok")
	(*log.PrefixedLogger).InfoDepth(pl, 1, name) // want "log message must be a constant; pass values as slog attributes"
	((*log.PrefixedLogger).Warn)(pl, name)       // want "log message must be a constant; pass values as slog attributes"
}

func promoted(b box, name string) {
	b.Warn("ok")
	b.Warn(name) // want "log message must be a constant; pass values as slog attributes"
	b.ErrorDepth(1, "ok")
	b.ErrorDepth(1, name) // want "log message must be a constant; pass values as slog attributes"
}

func promotedValue(b box) {
	warn := b.Warn // want "logging function must be called directly, not used as a value"
	warn("ok")
}

func storedMethodExpr(pl *log.PrefixedLogger) {
	fn := (*log.PrefixedLogger).Error // want "logging function must be called directly, not used as a value"
	fn(pl, "ok")
}
