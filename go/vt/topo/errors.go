/*
Copyright 2018 The Vitess Authors.

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

package topo

import (
	"fmt"
)

// ErrorCode is the error code for topo errors.
type ErrorCode int

// The following is the list of error codes.
const (
	NodeExists = ErrorCode(iota)
	NoNode
	NodeNotEmpty
	Timeout
	Interrupted
	BadVersion
	PartialResult
	NoUpdateNeeded
	NoImplementation
)

// Error represents a topo error.
type Error struct {
	code    ErrorCode
	message string
}

// NewError creates a new topo error.
func NewError(code ErrorCode, node string) error {
	var message string
	switch code {
	case NodeExists:
		message = fmt.Sprintf("node already exists: %s", node)
	case NoNode:
		message = fmt.Sprintf("node doesn't exist: %s", node)
	case NodeNotEmpty:
		message = fmt.Sprintf("node not empty: %s", node)
	case Timeout:
		message = fmt.Sprintf("deadline exceeded: %s", node)
	case Interrupted:
		message = fmt.Sprintf("interrupted: %s", node)
	case BadVersion:
		message = fmt.Sprintf("bad node version: %s", node)
	case PartialResult:
		message = fmt.Sprintf("partial result: %s", node)
	case NoUpdateNeeded:
		message = fmt.Sprintf("no update needed: %s", node)
	case NoImplementation:
		message = fmt.Sprintf("no such topology implementation %s", node)
	default:
		message = fmt.Sprintf("unknown code: %s", node)
	}
	return Error{
		code:    code,
		message: message,
	}
}

// Error satisfies error.
func (e Error) Error() string {
	return e.message
}

// IsErrType returns true if the error has the specified ErrorCode.
func IsErrType(err error, code ErrorCode) bool {
	if e, ok := err.(Error); ok {
		return e.code == code
	}
	return false
}
