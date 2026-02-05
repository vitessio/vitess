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

package grpcclient

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func captureOutput(t *testing.T, f func()) string {
	oldVal := os.Stderr
	t.Cleanup(func() {
		// Ensure reset even if deferred function panics
		os.Stderr = oldVal
	})

	r, w, err := os.Pipe()
	require.NoError(t, err)

	os.Stderr = w

	f()

	err = w.Close()
	require.NoError(t, err)

	got, err := io.ReadAll(r)
	require.NoError(t, err)

	return string(got)
}

func TestGlogger(t *testing.T) {
	gl := glogger{}

	output := captureOutput(t, func() {
		gl.Warning("warning")
	})
	require.Contains(t, output, "warning")

	output = captureOutput(t, func() {
		gl.Warningln("warningln")
	})
	require.Contains(t, output, "warningln\n")

	output = captureOutput(t, func() {
		gl.Warningf("formatted %s", "warning")
	})
	require.Contains(t, output, "formatted warning")

}

func TestGloggerError(t *testing.T) {
	gl := glogger{}

	output := captureOutput(t, func() {
		gl.Error("error message")
	})
	require.Contains(t, output, "error message")

	output = captureOutput(t, func() {
		gl.Errorln("error message line")
	})
	require.Contains(t, output, "error message line\n")

	output = captureOutput(t, func() {
		gl.Errorf("this is a %s error message", "formatted")
	})
	require.Contains(t, output, "this is a formatted error message")
}
