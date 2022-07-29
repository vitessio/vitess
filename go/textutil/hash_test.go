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

package textutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRandomHash(t *testing.T) {
	h := RandomHash()
	assert.Equal(t, randomHashSize, len(h))
}

func TestUUIDv5(t *testing.T) {
	assert.NotEmpty(t, UUIDv5("abc"))
	u := UUIDv5("abc", "def")
	assert.Equal(t, 36, len(u), "u=%v", u)
}

func TestUUIDv5Base36(t *testing.T) {
	assert.NotEmpty(t, UUIDv5("abc"))
	u := UUIDv5Base36("abc", "def")
	assert.Equal(t, 25, len(u), "u=%v", u)

	assert.Equal(t, UUIDv5Base36("abc", "def"), UUIDv5Base36("abc", "def"))     // verify deterministic
	assert.NotEqual(t, UUIDv5Base36("abc", "defg"), UUIDv5Base36("abc", "def")) // verify deterministic
	assert.Equal(t, "53qgoh2c0taw2uj43j5a0f0ul", UUIDv5Base36("abc", "def"))    // never change! The algorithm is fixed in stone
}
