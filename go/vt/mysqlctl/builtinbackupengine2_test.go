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

// Package mysqlctl_test is the blackbox tests for package mysqlctl.
package mysqlctl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetIncrementalFromPosGTIDSet(t *testing.T) {
	tcases := []struct {
		incrementalFromPos string
		gtidSet            string
		expctError         bool
	}{
		{
			"MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615",
			"16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615",
			false,
		},
		{
			"16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615",
			"16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615",
			false,
		},
		{
			"MySQL56/16b1039f-22b6-11ed-b765-0a43f95f28a3",
			"",
			true,
		},
		{
			"MySQL56/invalid",
			"",
			true,
		},
		{
			"16b1039f-22b6-11ed-b765-0a43f95f28a3",
			"",
			true,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.incrementalFromPos, func(t *testing.T) {
			gtidSet, err := getIncrementalFromPosGTIDSet(tcase.incrementalFromPos)
			if tcase.expctError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tcase.gtidSet, gtidSet.String())
			}
		})
	}
}
