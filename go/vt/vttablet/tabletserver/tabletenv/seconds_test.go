/*
Copyright 2020 The Vitess Authors.

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

package tabletenv

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/yaml2"
)

func TestSecondsYaml(t *testing.T) {
	type testSecond struct {
		Value Seconds `json:"value"`
	}

	ts := testSecond{
		Value: 1,
	}
	gotBytes, err := yaml2.Marshal(&ts)
	require.NoError(t, err)
	wantBytes := "value: 1\n"
	assert.Equal(t, wantBytes, string(gotBytes))

	var gotts testSecond
	err = yaml2.Unmarshal([]byte(wantBytes), &gotts)
	require.NoError(t, err)
	assert.Equal(t, ts, gotts)
}

func TestSecondsGetSet(t *testing.T) {
	var val Seconds
	val.Set(2 * time.Second)
	assert.Equal(t, Seconds(2), val)
	assert.Equal(t, 2*time.Second, val.Get())
}
