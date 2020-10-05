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

package onlineddl

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRandomHash(t *testing.T) {
	h1 := RandomHash()
	h2 := RandomHash()

	assert.Equal(t, len(h1), 64)
	assert.Equal(t, len(h2), 64)
	assert.NotEqual(t, h1, h2)
}

func TestToReadableTimestamp(t *testing.T) {
	ti, err := time.Parse(time.UnixDate, "Wed Feb 25 11:06:39 PST 2015")
	assert.NoError(t, err)

	readableTimestamp := ToReadableTimestamp(ti)
	assert.Equal(t, readableTimestamp, "20150225110639")
}
