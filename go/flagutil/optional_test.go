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

package flagutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewOptionalFloat64(t *testing.T) {
	fl := NewOptionalFloat64(4.187)
	require.NotEmpty(t, fl)
	require.Equal(t, false, fl.IsSet())

	require.Equal(t, "4.187", fl.String())
	require.Equal(t, "float64", fl.Type())

	err := fl.Set("invalid value")
	require.ErrorContains(t, err, "parse error")

	err = fl.Set("7.77")
	require.NoError(t, err)
	require.Equal(t, 7.77, fl.Get())
	require.Equal(t, true, fl.IsSet())

	err = fl.Set("1e1000")
	require.ErrorContains(t, err, "value out of range")
}

func TestNewOptionalString(t *testing.T) {
	optStr := NewOptionalString("4.187")
	require.NotEmpty(t, optStr)
	require.Equal(t, false, optStr.IsSet())

	require.Equal(t, "4.187", optStr.String())
	require.Equal(t, "string", optStr.Type())

	err := optStr.Set("value")
	require.NoError(t, err)

	require.Equal(t, "value", optStr.Get())
	require.Equal(t, true, optStr.IsSet())
}
