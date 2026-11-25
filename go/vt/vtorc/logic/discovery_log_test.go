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

package logic

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/vtorc/inst"
)

func TestShouldLogDiscoveryInfo(t *testing.T) {
	const lagThreshold int64 = 10

	newValidInstance := func() *inst.Instance {
		return &inst.Instance{
			Hostname:         "tablet-1",
			Port:             3306,
			ReadOnly:         true,
			IsLastCheckValid: true,
		}
	}

	t.Run("unchanged routine poll returns false", func(t *testing.T) {
		prev := newValidInstance()
		curr := newValidInstance()
		prev.SecondsBehindPrimary = sql.NullInt64{Int64: 0, Valid: true}
		curr.SecondsBehindPrimary = sql.NullInt64{Int64: lagThreshold, Valid: true}

		assert.False(t, shouldLogDiscoveryInfo(prev, curr, true, false, lagThreshold))
	})

	t.Run("changed routine poll returns true", func(t *testing.T) {
		prev := newValidInstance()
		curr := newValidInstance()
		curr.ReadOnly = false

		assert.True(t, shouldLogDiscoveryInfo(prev, curr, true, false, lagThreshold))
	})

	t.Run("lifecycle conditions return true", func(t *testing.T) {
		prev := newValidInstance()
		curr := newValidInstance()

		assert.True(t, shouldLogDiscoveryInfo(prev, curr, true, true, lagThreshold), "forceDiscovery")
		assert.True(t, shouldLogDiscoveryInfo(prev, curr, false, false, lagThreshold), "not found")
		assert.True(t, shouldLogDiscoveryInfo(nil, curr, true, false, lagThreshold), "nil prev")
		assert.True(t, shouldLogDiscoveryInfo(prev, nil, true, false, lagThreshold), "nil curr")

		invalidPrev := newValidInstance()
		invalidPrev.IsLastCheckValid = false
		assert.True(t, shouldLogDiscoveryInfo(invalidPrev, curr, true, false, lagThreshold), "recovery from invalid prev check")

		invalidCurr := newValidInstance()
		invalidCurr.IsLastCheckValid = false
		assert.True(t, shouldLogDiscoveryInfo(prev, invalidCurr, true, false, lagThreshold), "curr check invalid")
	})
}
