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
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

// TestFloatOrDuration_ValidFloat64Input verifies that a float64 input
// (representing seconds) is correctly converted to a time.Duration.
func TestFloatOrDuration_ValidFloat64Input(t *testing.T) {
	var duration time.Duration
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)

	FloatDuration(fs, &duration, "test_flag", 10*time.Second, "Test flag")
	err := fs.Parse([]string{"--test_flag=2"})
	assert.NoError(t, err)
	assert.Equal(t, 2*time.Second, duration)
}

// TestFloatOrDuration_ValidDurationInput verifies that a valid time.Duration
// input (e.g., "1m30s") is parsed and stored correctly.
func TestFloatOrDuration_ValidDurationInput(t *testing.T) {
	var duration time.Duration
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)

	FloatDuration(fs, &duration, "test_flag", 10*time.Second, "Test flag")
	err := fs.Parse([]string{"--test_flag=1m30s"})
	assert.NoError(t, err)
	assert.Equal(t, 90*time.Second, duration)
}

// TestFloatOrDuration_DefaultValue ensures that the default value is correctly
// assigned to the duration when the flag is not provided.
func TestFloatOrDuration_DefaultValue(t *testing.T) {
	var duration time.Duration
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)

	defaultValue := 15 * time.Second
	FloatDuration(fs, &duration, "test_flag", defaultValue, "Test flag")
	err := fs.Parse([]string{})
	assert.NoError(t, err)
	assert.Equal(t, defaultValue, duration)
}

// TestFloatOrDuration_InvalidInput verifies that an invalid input string
// results in an appropriate error.
func TestFloatOrDuration_InvalidInput(t *testing.T) {
	var duration time.Duration
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)

	FloatDuration(fs, &duration, "test_flag", 10*time.Second, "Test flag")
	err := fs.Parse([]string{"--test_flag=invalid"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "value must be either a float64 (interpreted as seconds) or a valid time.Duration")
}

// TestFloatOrDuration_MultipleFlags ensures that multiple FloatDuration flags
// can coexist and maintain independent values.
func TestFloatOrDuration_MultipleFlags(t *testing.T) {
	var duration1, duration2 time.Duration
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)

	FloatDuration(fs, &duration1, "flag1", 10*time.Second, "First test flag")
	FloatDuration(fs, &duration2, "flag2", 20*time.Second, "Second test flag")

	err := fs.Parse([]string{"--flag1=2.5", "--flag2=1m"})
	assert.NoError(t, err)
	assert.Equal(t, 2500*time.Millisecond, duration1)
	assert.Equal(t, 1*time.Minute, duration2)
}

// TestFloatOrDuration_HelpMessage verifies that the help message includes
// the correct flag name, description, and default value.
func TestFloatOrDuration_HelpMessage(t *testing.T) {
	var duration time.Duration
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)

	defaultValue := 10 * time.Second
	FloatDuration(fs, &duration, "test_flag", defaultValue, "Test flag with default value")

	helpOutput := fs.FlagUsages()
	assert.Contains(t, helpOutput, "--test_flag time.Duration")
	assert.Contains(t, helpOutput, "Test flag with default value")
	assert.Contains(t, helpOutput, "(default 10s)")
}
