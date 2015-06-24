// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package servenv

import "fmt"

// HandlePanic should be called using 'defer' in the RPC code that executes the command.
func HandlePanic(component string, err *error) {
	if x := recover(); x != nil {
		*err = fmt.Errorf("uncaught %v panic: %v", component, x)
	}
}
