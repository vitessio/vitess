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

package binlogplayer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestNewDBClientWithSidecarName(t *testing.T) {
	parser := sqlparser.NewTestParser()
	var params dbconfigs.Connector

	t.Run("default name returns dbClientImpl", func(t *testing.T) {
		client := NewDBClientWithSidecarName(params, parser, sidecar.DefaultName)
		_, ok := client.(*dbClientImpl)
		assert.True(t, ok, "expected *dbClientImpl for default sidecar name")
	})

	t.Run("custom name returns dbClientImplWithSidecarDBReplacement", func(t *testing.T) {
		client := NewDBClientWithSidecarName(params, parser, "_vt_custom_0")
		impl, ok := client.(*dbClientImplWithSidecarDBReplacement)
		assert.True(t, ok, "expected *dbClientImplWithSidecarDBReplacement for custom sidecar name")
		if ok {
			assert.Equal(t, "_vt_custom_0", impl.sidecarDBName)
		}
	})
}
