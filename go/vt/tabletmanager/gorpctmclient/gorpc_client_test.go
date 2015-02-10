// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpctmclient

import (
	"errors"
	"testing"
)

func TestIsTimeoutError(t *testing.T) {
	inputToWant := map[error]bool{
		timeoutError{errors.New("")}: true,
		errors.New(""):               false,
	}

	client := &GoRPCTabletManagerClient{}
	for input, want := range inputToWant {
		got := client.IsTimeoutError(input)
		if got != want {
			t.Errorf("IsTimoutError(%#v) => %v, want %v", input, got, want)
		}
	}
}
