/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vterrors

import (
	"fmt"

	"golang.org/x/net/context"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

type vtError struct {
	code vtrpcpb.Code
	err  string
}

// New creates a new error using the code and input string.
func New(code vtrpcpb.Code, in string) error {
	if code == vtrpcpb.Code_OK {
		panic("OK is an invalid error code; use INTERNAL instead")
	}
	return &vtError{
		code: code,
		err:  in,
	}
}

// Wrap wraps the given error, returning a new error with the given message as a prefix but with the same error code (if err was a vterror) and message of the passed error.
func Wrap(err error, message string) error {
	return New(Code(err), fmt.Sprintf("%v: %v", message, err.Error()))
}

// Wrapf wraps the given error, returning a new error with the given format string as a prefix but with the same error code (if err was a vterror) and message of the passed error.
func Wrapf(err error, format string, args ...interface{}) error {
	return Wrap(err, fmt.Sprintf(format, args...))
}

// Errorf returns a new error built using Printf style arguments.
func Errorf(code vtrpcpb.Code, format string, args ...interface{}) error {
	return New(code, fmt.Sprintf(format, args...))
}

func (e *vtError) Error() string {
	return e.err
}

// Code returns the error code if it's a vtError.
// If err is nil, it returns ok. Otherwise, it returns unknown.
func Code(err error) vtrpcpb.Code {
	if err == nil {
		return vtrpcpb.Code_OK
	}
	if err, ok := err.(*vtError); ok {
		return err.code
	}
	// Handle some special cases.
	switch err {
	case context.Canceled:
		return vtrpcpb.Code_CANCELED
	case context.DeadlineExceeded:
		return vtrpcpb.Code_DEADLINE_EXCEEDED
	}
	return vtrpcpb.Code_UNKNOWN
}

// Equals returns true iff the error message and the code returned by Code()
// is equal.
func Equals(a, b error) bool {
	if a == nil && b == nil {
		// Both are nil.
		return true
	}

	if a == nil && b != nil || a != nil && b == nil {
		// One of the two is nil.
		return false
	}

	return a.Error() == b.Error() && Code(a) == Code(b)
}

// Print is meant to print the vtError object in test failures.
// For comparing two vterrors, use Equals() instead.
func Print(err error) string {
	return fmt.Sprintf("%v: %v", Code(err), err.Error())
}
