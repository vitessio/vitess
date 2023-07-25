/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
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
