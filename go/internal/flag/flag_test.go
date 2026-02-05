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

package flag

import (
	goflag "flag"
	"os"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

func TestPreventGlogVFlagFromClobberingVersionFlagShorthand(t *testing.T) {
	oldCommandLine := goflag.CommandLine
	defer func() {
		goflag.CommandLine = oldCommandLine
	}()

	goflag.CommandLine = goflag.NewFlagSet(os.Args[0], goflag.ExitOnError)

	var v bool

	goflag.BoolVar(&v, "v", true, "")

	testFlagSet := pflag.NewFlagSet("testFlagSet", pflag.ExitOnError)
	PreventGlogVFlagFromClobberingVersionFlagShorthand(testFlagSet)

	f := testFlagSet.Lookup("v")
	assert.NotNil(t, f)
	assert.Equal(t, "", f.Shorthand)

	// The function should not panic if -v flag is already defined
	assert.NotPanics(t, func() { PreventGlogVFlagFromClobberingVersionFlagShorthand(testFlagSet) })
}

func TestParse(t *testing.T) {
	oldCommandLine := goflag.CommandLine
	defer func() {
		goflag.CommandLine = oldCommandLine
	}()

	var testFlag bool
	goflag.CommandLine = goflag.NewFlagSet(os.Args[0], goflag.ExitOnError)
	goflag.BoolVar(&testFlag, "testFlag", true, "")

	testFlagSet := pflag.NewFlagSet("testFlagSet", pflag.ExitOnError)

	Parse(testFlagSet)

	f := testFlagSet.ShorthandLookup("h")
	assert.NotNil(t, f)
	assert.Equal(t, "false", f.DefValue)

	f = testFlagSet.Lookup("help")
	assert.NotNil(t, f)
	assert.Equal(t, "false", f.DefValue)

	testFlagSet = pflag.NewFlagSet("testFlagSet2", pflag.ExitOnError)

	// If shorthand "h" is already defined, shorthand for "help" should be empty
	var h bool
	testFlagSet.BoolVarP(&h, "testH", "h", false, "")

	Parse(testFlagSet)
	f = testFlagSet.Lookup("help")
	assert.NotNil(t, f)
	assert.Equal(t, "", f.Shorthand)

	// Check if AddGoFlagSet was called
	f = testFlagSet.Lookup("testFlag")
	assert.NotNil(t, f)
	assert.Equal(t, "true", f.DefValue)
}

func TestIsFlagProvided(t *testing.T) {
	oldPflagCommandLine := pflag.CommandLine
	defer func() {
		pflag.CommandLine = oldPflagCommandLine
	}()

	pflag.CommandLine = pflag.NewFlagSet("testFlagSet", pflag.ExitOnError)

	flagName := "testFlag"
	isProvided := IsFlagProvided(flagName)
	assert.False(t, isProvided, "flag %q should not exist", flagName)

	var testFlag bool
	pflag.BoolVar(&testFlag, flagName, false, "")

	// Should return false as testFlag is not set
	isProvided = IsFlagProvided(flagName)
	assert.False(t, isProvided, "flag %q should not be provided", flagName)

	pflag.Parse()
	_ = pflag.Set(flagName, "true")

	// Should return true as testFlag is set
	isProvided = IsFlagProvided(flagName)
	assert.True(t, isProvided, "flag %q should be provided", flagName)
}

func TestFilterTestFlags(t *testing.T) {
	oldOsArgs := os.Args
	defer func() {
		os.Args = oldOsArgs
	}()

	os.Args = []string{
		"-test.run",
		"TestFilter",
		"otherArgs1",
		"otherArgs2",
		"-test.run=TestFilter",
	}

	otherArgs, testFlags := filterTestFlags()

	expectedTestFlags := []string{
		"-test.run",
		"TestFilter",
		"-test.run=TestFilter",
	}
	expectedOtherArgs := []string{
		"otherArgs1",
		"otherArgs2",
	}

	assert.Equal(t, expectedOtherArgs, otherArgs)
	assert.Equal(t, expectedTestFlags, testFlags)
}

func TestParseFlagsForTest(t *testing.T) {
	oldOsArgs := os.Args
	oldPflagCommandLine := pflag.CommandLine
	oldCommandLine := goflag.CommandLine

	defer func() {
		os.Args = oldOsArgs
		pflag.CommandLine = oldPflagCommandLine
		goflag.CommandLine = oldCommandLine
	}()

	pflag.CommandLine = pflag.NewFlagSet("testFlagSet", pflag.ExitOnError)

	os.Args = []string{
		"-test.run",
		"TestFilter",
		"otherArgs1",
		"otherArgs2",
		"-test.run=TestFilter",
	}

	ParseFlagsForTest()

	expectedOsArgs := []string{
		"otherArgs1",
		"otherArgs2",
	}

	assert.Equal(t, expectedOsArgs, os.Args)
	assert.Equal(t, true, pflag.Parsed())
}

func TestParsed(t *testing.T) {
	oldPflagCommandLine := pflag.CommandLine
	oldCommandLine := goflag.CommandLine

	defer func() {
		pflag.CommandLine = oldPflagCommandLine
		goflag.CommandLine = oldCommandLine
	}()

	pflag.CommandLine = pflag.NewFlagSet("testPflagSet", pflag.ExitOnError)
	goflag.CommandLine = goflag.NewFlagSet("testGoflagSet", goflag.ExitOnError)

	b := Parsed()
	assert.False(t, b, "command-line flags should not be parsed")

	pflag.Parse()
	b = Parsed()
	assert.True(t, b, "command-line flags should be parsed")
}

func TestLookup(t *testing.T) {
	oldPflagCommandLine := pflag.CommandLine
	oldCommandLine := goflag.CommandLine

	defer func() {
		pflag.CommandLine = oldPflagCommandLine
		goflag.CommandLine = oldCommandLine
	}()

	pflag.CommandLine = pflag.NewFlagSet("testPflagSet", pflag.ExitOnError)
	goflag.CommandLine = goflag.NewFlagSet("testGoflagSet", goflag.ExitOnError)

	var testGoFlag, testPflag, testFlag bool

	goflag.BoolVar(&testGoFlag, "testGoFlag", true, "")
	goflag.BoolVar(&testFlag, "t", true, "")
	pflag.BoolVar(&testPflag, "testPflag", true, "")

	testCases := []struct {
		shorthand string
		name      string
	}{
		{
			// If single character flag is passed, the shorthand should be the same
			shorthand: "t",
			name:      "t",
		},
		{
			shorthand: "",
			name:      "testGoFlag",
		},
		{
			shorthand: "",
			name:      "testPflag",
		},
	}

	for _, tt := range testCases {
		f := Lookup(tt.name)

		assert.NotNil(t, f)
		assert.Equal(t, tt.shorthand, f.Shorthand)
		assert.Equal(t, tt.name, f.Name)
	}

	f := Lookup("non-existent-flag")
	assert.Nil(t, f)
}

func TestArgs(t *testing.T) {
	oldPflagCommandLine := pflag.CommandLine
	oldOsArgs := os.Args

	defer func() {
		pflag.CommandLine = oldPflagCommandLine
		os.Args = oldOsArgs
	}()

	pflag.CommandLine = pflag.NewFlagSet("testPflagSet", pflag.ExitOnError)

	os.Args = []string{
		"arg0",
		"arg1",
		"arg2",
		"arg3",
	}

	expectedArgs := []string{
		"arg1",
		"arg2",
		"arg3",
	}

	pflag.Parse()
	// Should work equivalent to pflag.Args if there's no double dash
	args := Args()
	assert.Equal(t, expectedArgs, args)

	arg := Arg(2)
	assert.Equal(t, "arg3", arg)

	// Should return empty string if the index is greater than len of CommandLine.args
	arg = Arg(3)
	assert.Equal(t, "", arg)
}

func TestIsZeroValue(t *testing.T) {
	var testFlag string

	testFlagSet := goflag.NewFlagSet("testFlagSet", goflag.ExitOnError)
	testFlagSet.StringVar(&testFlag, "testflag", "default", "Description of testflag")

	f := testFlagSet.Lookup("testflag")

	result := isZeroValue(f, "")
	assert.True(t, result, "empty string should represent zero value for string flag")

	result = isZeroValue(f, "anyValue")
	assert.False(t, result, "non-empty string should not represent zero value for string flag")
}
