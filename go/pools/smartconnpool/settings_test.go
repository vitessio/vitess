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

package smartconnpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A setting covers another when it assigns every variable the other assigns,
// whatever order either names them in; a setting that names no variables is
// neither covering nor covered, since nothing is known about what it assigns.
func TestSettingCovers(t *testing.T) {
	withVariables := func(names ...string) *Setting {
		return NewSettingWithOptions("apply", "reset", SettingOptions{Variables: names})
	}
	a := withVariables("sql_safe_updates")
	ab := withVariables("sql_select_limit", "sql_safe_updates", "sql_select_limit")
	c := withVariables("sql_mode")
	unnamed := NewSetting("apply", "reset")

	assert.True(t, ab.Covers(a), "a superset covers")
	assert.True(t, ab.Covers(ab), "a setting covers itself")
	assert.True(t, ab.Covers(withVariables("sql_select_limit", "sql_safe_updates")), "order and duplicates do not matter")
	assert.False(t, a.Covers(ab), "a subset does not cover")
	assert.False(t, ab.Covers(c), "a disjoint setting does not cover")
	assert.False(t, withVariables("sql_mode", "sql_safe_updates").Covers(ab), "an overlapping setting does not cover")
	assert.False(t, unnamed.Covers(a))
	assert.False(t, ab.Covers(unnamed))
}
