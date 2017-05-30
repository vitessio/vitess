/*
Copyright 2017 Google Inc.

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

package servenv

import (
	"fmt"
	"strings"

	"github.com/youtube/vitess/go/tb"
)

// HandlePanic should be called using 'defer' in the RPC code that executes the command.
func HandlePanic(component string, err *error) {
	if x := recover(); x != nil {
		// gRPC 0.13 chokes when you return a streaming error that contains newlines.
		*err = fmt.Errorf("uncaught %v panic: %v, %s", component, x,
			strings.Replace(string(tb.Stack(4)), "\n", ";", -1))
	}
}
