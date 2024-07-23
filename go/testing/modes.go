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

package testing

type TestingMode int

const (
	Production TestingMode = iota
	Debug2PC
)

// Mode is used to define whether we are in Production mode or testing mode.
// We use go build directives to include files that change the testing mode when certain tags are provided
// while building binaries. This allows to have debugging code written in normal code flow without affecting
// production performance.
// Usage - To add debug code, put it behind testing.Mode != Production etc checks.
var Mode TestingMode = Production

// String is an unused function, but can be used in debugging to see what the testing mode is.
func (tm TestingMode) String() string {
	switch tm {
	case Production:
		return "Production"
	case Debug2PC:
		return "Debug2PC"
	}
	return "Unknown"
}
