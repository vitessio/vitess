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
	queryApply string
	queryReset string
	bucket     uint32
}

func (s *Setting) ApplyQuery() string {
	return s.queryApply
}

func (s *Setting) ResetQuery() string {
	return s.queryReset
}

var globalSettingsCounter atomic.Uint32

func NewSetting(apply, reset string) *Setting {
	return &Setting{apply, reset, globalSettingsCounter.Add(1)}
}
