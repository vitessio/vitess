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

package flagutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStringSetFlag(t *testing.T) {
	strSetFlag := StringSetFlag{}
	set := strSetFlag.ToSet()
	require.Empty(t, set)

	set = set.Insert("mango", "apple", "mango")
	strSetFlag.set = set

	require.Equal(t, "StringSetFlag", strSetFlag.Type())
	require.Equal(t, "apple, mango", strSetFlag.String())

	err := strSetFlag.Set("guvava")
	require.NoError(t, err)
	require.Equal(t, "apple, guvava, mango", strSetFlag.String())

	require.NotEmpty(t, strSetFlag.ToSet())
}

func TestStringSetFlagWithEmptySet(t *testing.T) {
	strSetFlag := StringSetFlag{}
	require.Equal(t, "", strSetFlag.String())

	err := strSetFlag.Set("tmp")
	require.NoError(t, err)
	require.Empty(t, strSetFlag.ToSet())

	err = strSetFlag.Set("guvava")
	require.NoError(t, err)
	require.Equal(t, "guvava", strSetFlag.String())
}
