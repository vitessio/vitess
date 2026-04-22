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
	"strconv"
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

func TestNewOptionalFlag_Generic(t *testing.T) {
	flag := NewOptionalFlag(
		42,
		"int",
		func(s string) (int, error) { return strconv.Atoi(s) },
		func(v int) string { return strconv.Itoa(v) },
	)

	require.NotNil(t, flag)
	require.False(t, flag.IsSet())
	require.Equal(t, "42", flag.String())
	require.Equal(t, "int", flag.Type())
	require.Equal(t, 42, flag.Get())

	err := flag.Set("100")
	require.NoError(t, err)
	require.Equal(t, 100, flag.Get())
	require.True(t, flag.IsSet())

	err = flag.Set("not-a-number")
	require.Error(t, err)
}

func TestOptionalFlag_Compatibility(t *testing.T) {
	// OptionalFlag interface is still satisfied by both concrete alias types.
	var iface OptionalFlag

	iface = NewOptionalFloat64(3.14)
	require.NotNil(t, iface)
	require.False(t, iface.IsSet())

	iface = NewOptionalString("hello")
	require.NotNil(t, iface)
	require.False(t, iface.IsSet())

	// Old type names are still valid (backward compat via type alias).
	var f64 *OptionalFloat64
	f64 = NewOptionalFloat64(1.0)
	require.NotNil(t, f64)

	var str *OptionalString
	str = NewOptionalString("world")
	require.NotNil(t, str)
}

func TestNewOptionalFlag_NilParsePanics(t *testing.T) {
	require.Panics(t, func() {
		NewOptionalFlag(0, "int", nil, func(v int) string { return strconv.Itoa(v) })
	})
}

func TestOptionalFlagValue_ZeroValue(t *testing.T) {
	// A zero-value struct must not panic.
	var f OptionalFlagValue[int]

	// String() falls back to fmt.Sprintf, not panic.
	require.Equal(t, "0", f.String())

	// Set() returns an error, not panic.
	err := f.Set("42")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no parse function")
}
