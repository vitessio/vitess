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
	"slices"
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
	variables   []string
	sqlMode     uint64
	setsSQLMode bool
}

func (s *Setting) ApplyQuery() string {
	return s.queryApply
}

func (s *Setting) ResetQuery() string {
	return s.queryReset
}

// SQLMode returns the sql_mode these settings put the session in, as the lexer
// modes the caller's parser honors (a sqlmode.Mode bitmask, opaque to this
// package): the caller parses SQL on the connection under them.
func (s *Setting) SQLMode() uint64 {
	return s.sqlMode
}

// SetsSQLMode reports whether the settings assign sql_mode at all. Settings that
// do not assign it leave a connection's session in whatever mode it is already
// in, which the connection's recorded parse-relevant bits describe, not SQLMode.
func (s *Setting) SetsSQLMode() bool {
	return s.setsSQLMode
}

// Covers reports whether s assigns every variable other assigns, so that
// applying s on a connection that carries other leaves the session exactly as s
// describes it, with none of other's variables in effect that s does not name.
// Settings that do not name their variables cover nothing and are covered by
// nothing.
func (s *Setting) Covers(other *Setting) bool {
	if len(s.variables) == 0 || len(other.variables) == 0 {
		return false
	}
	i := 0
	for _, name := range other.variables {
		for i < len(s.variables) && s.variables[i] < name {
			i++
		}
		if i == len(s.variables) || s.variables[i] != name {
			return false
		}
	}
	return true
}

var globalSettingsCounter atomic.Uint32

func NewSetting(apply, reset string) *Setting {
	return NewSettingWithOptions(apply, reset, SettingOptions{})
}

// SettingOptions describes what a setting does beyond its queries. The zero
// value describes a setting the pool knows nothing more about.
type SettingOptions struct {
	// Variables names the variables the setting assigns, in any order and
	// spelled as the caller compares them (see Setting.Covers).
	Variables []string
	// SetsSQLMode reports whether the setting assigns sql_mode, and SQLMode the
	// lexer modes it then puts the session in (see Setting.SQLMode).
	SetsSQLMode bool
	SQLMode     uint64
}

// NewSettingWithOptions is NewSetting for a setting described by opts.
func NewSettingWithOptions(apply, reset string, opts SettingOptions) *Setting {
	var variables []string
	if len(opts.Variables) > 0 {
		variables = slices.Clone(opts.Variables)
		slices.Sort(variables)
		variables = slices.Compact(variables)
	}
	return &Setting{
		queryApply:  apply,
		queryReset:  reset,
		bucket:      globalSettingsCounter.Add(1),
		variables:   variables,
		sqlMode:     opts.SQLMode,
		setsSQLMode: opts.SetsSQLMode,
	}
}
