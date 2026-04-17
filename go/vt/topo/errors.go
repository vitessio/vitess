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

package topo

import (
	"errors"
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
	NoReadOnlyImplementation
	ResourceExhausted
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
		message = "node already exists: " + node
	case NoNode:
		message = "node doesn't exist: " + node
	case NodeNotEmpty:
		message = "node not empty: " + node
	case Timeout:
		message = "deadline exceeded: " + node
	case Interrupted:
		message = "interrupted: " + node
	case BadVersion:
		message = "bad node version: " + node
	case PartialResult:
		message = "partial result: " + node
	case NoUpdateNeeded:
		message = "no update needed: " + node
	case NoImplementation:
		message = "no such topology implementation " + node
	case NoReadOnlyImplementation:
		message = "no read-only topology implementation " + node
	case ResourceExhausted:
		message = "server resource exhausted: " + node
	default:
		message = "unknown code: " + node
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
	var e Error

	if errors.As(err, &e) {
		return e.code == code
	}

	if e, ok := err.(Error); ok {
		return e.code == code
	}

	return false
}
