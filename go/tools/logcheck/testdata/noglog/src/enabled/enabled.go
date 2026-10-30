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

package enabled

import (
	"log/slog"

	"vitess.io/vitess/go/vt/log"
)

func debug(name string) {
	if log.Enabled(slog.LevelDebug) {
		log.Debug("ok", slog.String("name", name))
		log.Debug(name) // want "log message must be a constant; pass values as slog attributes"
	}
	enabled := log.Enabled
	_ = enabled(slog.LevelInfo)
}

func prefixed(name string) {
	pl := log.NewPrefixedLogger("component")
	pl.Info("ok")
	pl.Info(name)          // want "log message must be a constant; pass values as slog attributes"
	pl.ErrorDepth(1, name) // want "log message must be a constant; pass values as slog attributes"
}
