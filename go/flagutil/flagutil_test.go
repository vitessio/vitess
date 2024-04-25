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
	"strings"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStringList(t *testing.T) {
	p := StringListValue([]string{})
	var _ pflag.Value = &p
	wanted := map[string]string{
		"0ala,ma,kota":   "0ala.ma.kota",
		`1ala\,ma,kota`:  "1ala,ma.kota",
		`2ala\\,ma,kota`: `2ala\.ma.kota`,
		"3ala,":          "3ala.",
	}
	for in, out := range wanted {
		err := p.Set(in)
		assert.NoError(t, err)

		assert.Equal(t, out, strings.Join(p, "."))
		assert.Equal(t, in, p.String())

	}
}

// TestEmptyStringList verifies that an empty parameter results in an empty list
func TestEmptyStringList(t *testing.T) {
	var p StringListValue
	var _ pflag.Value = &p

	err := p.Set("")
	require.NoError(t, err)
	require.Len(t, p, 0)
}

type pair struct {
	in  string
	out map[string]string
	err error
}

func TestStringMap(t *testing.T) {
	v := StringMapValue(nil)
	var _ pflag.Value = &v
	wanted := []pair{
		{
			in:  "tag1:value1,tag2:value2",
			out: map[string]string{"tag1": "value1", "tag2": "value2"},
		},
		{
			in:  `tag1:1:value1\,,tag2:value2`,
			out: map[string]string{"tag1": "1:value1,", "tag2": "value2"},
		},
		{
			in:  `tag1:1:value1\,,tag2`,
			err: errInvalidKeyValuePair,
		},
	}
	for _, want := range wanted {
		err := v.Set(want.in)
		assert.ErrorIs(t, err, want.err)

		if want.err != nil {
			continue
		}

		assert.EqualValues(t, want.out, v)
		assert.Equal(t, want.in, v.String())
	}
}

func TestStringListValue(t *testing.T) {
	strListVal := StringListValue{"temp", "val"}
	require.Equal(t, []string([]string{"temp", "val"}), strListVal.Get())
	require.Equal(t, "strings", strListVal.Type())
}

func TestStringMapValue(t *testing.T) {
	strMapVal := StringMapValue{
		"key": "val",
	}
	require.Equal(t, "StringMap", strMapVal.Type())
	require.Equal(t, map[string]string(map[string]string{"key": "val"}), strMapVal.Get())
}

func TestDualFormatStringListVar(t *testing.T) {
	testFlagSet := pflag.NewFlagSet("testFlagSet", pflag.ExitOnError)

	testFlagName := "test-flag_name"
	var flagVal []string
	testValue := []string{"testValue1", "testValue2", "testValue3"}

	DualFormatStringListVar(testFlagSet, &flagVal, testFlagName, testValue, "usage string")
	assert.Equal(t, testValue, flagVal)

	want := "testValue1,testValue2,testValue3"
	f := testFlagSet.Lookup("test-flag-name")
	assert.NotNil(t, f)
	assert.Equal(t, want, f.Value.String())

	f = testFlagSet.Lookup("test_flag_name")
	assert.NotNil(t, f)
	assert.Equal(t, want, f.Value.String())

	newVal := "newValue1,newValue2"
	err := testFlagSet.Set("test-flag-name", newVal)
	assert.NoError(t, err)

	assert.Equal(t, newVal, f.Value.String())
	assert.Equal(t, []string{"newValue1", "newValue2"}, flagVal)
}

func TestDualFormatStringVar(t *testing.T) {
	testFlagSet := pflag.NewFlagSet("testFlagSet", pflag.ExitOnError)

	testFlagName := "test-flag_name"
	var flagVal string
	testValue := "testValue"

	DualFormatStringVar(testFlagSet, &flagVal, testFlagName, testValue, "usage string")
	assert.Equal(t, testValue, flagVal)

	f := testFlagSet.Lookup("test-flag-name")
	assert.NotNil(t, f)
	assert.Equal(t, testValue, f.Value.String())

	f = testFlagSet.Lookup("test_flag_name")
	assert.NotNil(t, f)
	assert.Equal(t, testValue, f.Value.String())

	newVal := "newValue"
	err := testFlagSet.Set("test-flag-name", newVal)
	assert.NoError(t, err)

	assert.Equal(t, newVal, f.Value.String())
	assert.Equal(t, newVal, flagVal)
}

func TestDualFormatBoolVar(t *testing.T) {
	testFlagSet := pflag.NewFlagSet("testFlagSet", pflag.ExitOnError)

	testFlagName := "test-flag_name"
	var flagVal bool

	DualFormatBoolVar(testFlagSet, &flagVal, testFlagName, true, "usage string")
	assert.True(t, flagVal)

	f := testFlagSet.Lookup("test-flag-name")
	assert.NotNil(t, f)
	assert.Equal(t, "true", f.Value.String())

	f = testFlagSet.Lookup("test_flag_name")
	assert.NotNil(t, f)
	assert.Equal(t, "true", f.Value.String())

	err := testFlagSet.Set("test-flag-name", "false")
	assert.NoError(t, err)

	assert.Equal(t, "false", f.Value.String())
	assert.False(t, flagVal)
}

func TestDualFormatInt64Var(t *testing.T) {
	testFlagSet := pflag.NewFlagSet("testFlagSet", pflag.ExitOnError)

	testFlagName := "test-flag_name"
	var flagVal int64

	DualFormatInt64Var(testFlagSet, &flagVal, testFlagName, int64(256), "usage string")
	assert.Equal(t, int64(256), flagVal)

	f := testFlagSet.Lookup("test-flag-name")
	assert.NotNil(t, f)
	assert.Equal(t, "256", f.Value.String())

	f = testFlagSet.Lookup("test_flag_name")
	assert.NotNil(t, f)
	assert.Equal(t, "256", f.Value.String())

	newVal := "128"
	err := testFlagSet.Set("test-flag-name", newVal)
	assert.NoError(t, err)

	assert.Equal(t, newVal, f.Value.String())
	assert.Equal(t, int64(128), flagVal)
}

func TestDualFormatIntVar(t *testing.T) {
	testFlagSet := pflag.NewFlagSet("testFlagSet", pflag.ExitOnError)

	testFlagName := "test-flag_name"
	var flagVal int

	DualFormatIntVar(testFlagSet, &flagVal, testFlagName, 128, "usage string")
	assert.Equal(t, 128, flagVal)

	f := testFlagSet.Lookup("test-flag-name")
	assert.NotNil(t, f)
	assert.Equal(t, "128", f.Value.String())

	f = testFlagSet.Lookup("test_flag_name")
	assert.NotNil(t, f)
	assert.Equal(t, "128", f.Value.String())

	newVal := "256"
	err := testFlagSet.Set("test-flag-name", newVal)
	assert.NoError(t, err)

	assert.Equal(t, newVal, f.Value.String())
	assert.Equal(t, 256, flagVal)
}

type MockValue struct {
	val *bool
}

func (b MockValue) Set(s string) error {
	if s == "true" {
		*b.val = true
	} else {
		*b.val = false
	}
	return nil
}

func (b MockValue) String() string {
	if *b.val {
		return "true"
	}
	return "false"
}

func (b MockValue) Type() string {
	return "bool"
}

func TestDualFormatVar(t *testing.T) {
	testFlagSet := pflag.NewFlagSet("testFlagSet", pflag.ExitOnError)

	testFlagName := "test-flag_name"
	flagVal := true
	value := MockValue{val: &flagVal}

	DualFormatVar(testFlagSet, value, testFlagName, "usage string")

	f := testFlagSet.Lookup("test-flag-name")
	assert.NotNil(t, f)
	assert.Equal(t, "true", f.Value.String())

	f = testFlagSet.Lookup("test_flag_name")
	assert.NotNil(t, f)
	assert.Equal(t, "true", f.Value.String())

	newVal := "false"
	err := testFlagSet.Set("test-flag-name", newVal)
	assert.NoError(t, err)

	assert.Equal(t, newVal, f.Value.String())
	assert.False(t, flagVal)
}
