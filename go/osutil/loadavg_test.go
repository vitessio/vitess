/*
Copyright 2024 The Vitess Authors.

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

package osutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadAvgValue(t *testing.T) {
	tcases := []struct {
		input   string
		loadavg float64
		isError bool
	}{
		{
			input:   "",
			isError: true,
		},
		{
			input:   "{}",
			isError: true,
		},
		{
			input:   "{ x y z }",
			isError: true,
		},
		{
			input:   "1",
			loadavg: 1.0,
		},
		{
			input:   "0.00 0.00 0.00 1/1 1",
			loadavg: 0.0,
		},
		{
			input:   "2.72 2.89 3.17",
			loadavg: 2.72,
		},
		{
			input:   "   2.72 2.89 3.17",
			loadavg: 2.72,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.input, func(t *testing.T) {
			loadavg, err := parseLoadAvg(tcase.input)
			if tcase.isError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tcase.loadavg, loadavg)
			}
		})
	}
}

func TestLoadAvg(t *testing.T) {
	loadavg, err := LoadAvg()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, loadavg, 0.0)
}
