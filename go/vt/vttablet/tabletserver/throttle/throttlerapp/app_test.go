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

package throttlerapp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExemptFromChecks(t *testing.T) {
	tcases := map[string]bool{
		"":                         false,
		VReplicationName.String():  false,
		VPlayerName.String():       false,
		VCopierName.String():       false,
		OnlineDDLName.String():     false,
		BinlogWatcherName.String(): true,
		MessagerName.String():      true,
		SchemaTrackerName.String(): true,
	}
	for app, expectExempt := range tcases {
		t.Run(app, func(t *testing.T) {
			exempt := ExemptFromChecks(app)
			assert.Equal(t, expectExempt, exempt)
		})
	}
}

func TestConcatenate(t *testing.T) {
	n := VReplicationName
	vcopierName := n.Concatenate("vcopier")
	assert.Equal(t, Name("vreplication:vcopier"), vcopierName)
	vplayerName := n.Concatenate("vplayer")
	assert.Equal(t, Name("vreplication:vplayer"), vplayerName)
	rowstreamerName := n.Concatenate(RowStreamerName)
	assert.Equal(t, Name("vreplication:rowstreamer"), rowstreamerName)
	assert.Equal(t, "vreplication:rowstreamer", rowstreamerName.String())
}
