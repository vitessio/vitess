/*
Copyright 2019 The Vitess Authors.

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

package stats

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetOrNewCounter(t *testing.T) {

	c1 := GetOrNewCounter("size_c", "help message")
	require.NotNil(t, c1)
	assert.Equal(t, int64(0), c1.Get())
	c1.Add(2)
	assert.Equal(t, int64(2), c1.Get())

	c2 := GetOrNewCounter("size_c", "help message")
	assert.Equal(t, int64(2), c1.Get())
	assert.Equal(t, int64(2), c2.Get())
	assert.Equal(t, c1, c2)
	c1.Add(3)
	assert.Equal(t, int64(5), c1.Get())
	assert.Equal(t, int64(5), c2.Get())
}

func TestGetOrNewGauge(t *testing.T) {

	c1 := GetOrNewGauge("size_g", "help message")
	require.NotNil(t, c1)
	assert.Equal(t, int64(0), c1.Get())
	c1.Add(2)
	assert.Equal(t, int64(2), c1.Get())

	c2 := GetOrNewGauge("size_g", "help message")
	assert.Equal(t, int64(2), c1.Get())
	assert.Equal(t, int64(2), c2.Get())
	assert.Equal(t, c1, c2)
	c1.Add(3)
	assert.Equal(t, int64(5), c1.Get())
	assert.Equal(t, int64(5), c2.Get())
}

func TestGetOrNewGaugeFloat64(t *testing.T) {

	c1 := GetOrNewGaugeFloat64("size_gf64", "help message")
	require.NotNil(t, c1)
	assert.Equal(t, float64(0), c1.Get())
	c1.Set(3.14)
	assert.Equal(t, float64(3.14), c1.Get())

	c2 := GetOrNewGaugeFloat64("size_gf64", "help message")
	assert.Equal(t, float64(3.14), c1.Get())
	assert.Equal(t, float64(3.14), c2.Get())
	assert.Equal(t, c1, c2)
	c1.Set(2.718)
	assert.Equal(t, float64(2.718), c1.Get())
	assert.Equal(t, float64(2.718), c2.Get())
}
