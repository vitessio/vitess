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

package opentsdb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileWriter(t *testing.T) {
	tempFile, err := os.CreateTemp("", "tempfile")
	assert.NoError(t, err)
	defer os.Remove(tempFile.Name())

	w, err := newFileWriter(tempFile.Name())
	assert.NoError(t, err)

	dp := []*DataPoint{
		{
			Metric:    "testMetric",
			Timestamp: 1.0,
			Value:     2.0,
			Tags: map[string]string{
				"key1": "value1",
			},
		},
	}

	err = w.Write(dp)
	assert.NoError(t, err)

	err = tempFile.Close()
	assert.NoError(t, err)

	content, err := os.ReadFile(tempFile.Name())
	assert.NoError(t, err)
	assert.Equal(t, "testMetric 1.000000 2.000000 key1=value1\n", string(content))
}
