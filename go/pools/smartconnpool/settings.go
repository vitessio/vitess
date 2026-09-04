/*
Copyright 2023 The Vitess Authors.

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

package smartconnpool

import (
	"sync/atomic"
)

// Setting is a setting applied to a connection in this pool.
// Setting values must be interned for optimal usage (i.e. a Setting
// that represents a specific set of SQL connection settings should
// always have the same pointer value).
type Setting struct {
	queryApply  string
	queryReset  string
	bucket      uint32
	sqlMode     uint32
	setsSQLMode bool
}

func (s *Setting) ApplyQuery() string {
	return s.queryApply
}

func (s *Setting) ResetQuery() string {
	return s.queryReset
}

// SQLMode returns the parse-relevant sql_mode bits these settings put the
// session in (a sqlparser.SQLMode bitmask, opaque to this package). The apply
// query itself carries a value with those modes removed: the caller parses SQL
// under them and sends mode-independent text over the connection.
func (s *Setting) SQLMode() uint32 {
	return s.sqlMode
}

// SetsSQLMode reports whether the settings assign sql_mode at all. Settings that
// do not leave a connection's session in whatever mode it already is, which its
// recorded parse-relevant bits, not this setting's, describe.
func (s *Setting) SetsSQLMode() bool {
	return s.setsSQLMode
}

var globalSettingsCounter atomic.Uint32

func NewSetting(apply, reset string) *Setting {
	return &Setting{queryApply: apply, queryReset: reset, bucket: globalSettingsCounter.Add(1)}
}

// NewSettingWithSQLMode is NewSetting for settings that assign sql_mode, whose
// session then carries the given parse-relevant sql_mode bits.
func NewSettingWithSQLMode(apply, reset string, sqlMode uint32) *Setting {
	return &Setting{queryApply: apply, queryReset: reset, bucket: globalSettingsCounter.Add(1), sqlMode: sqlMode, setsSQLMode: true}
}
