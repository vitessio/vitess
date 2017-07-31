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
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/logutil"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

var logger = logutil.NewThrottledLogger("vterror", 5*time.Second)

type vtError struct {
	code vtrpcpb.Code
	err  string
}

// New creates a new error using the code and input string.
func New(code vtrpcpb.Code, in string) error {
	if code == vtrpcpb.Code_OK {
		logger.Errorf("OK is an invalid code, using INTERNAL instead: %s\n%s", in, tb.Stack(2))
		code = vtrpcpb.Code_INTERNAL
	}
	return &vtError{
		code: code,
		err:  in,
	}
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

// Suffix adds a suffix to a vtError
func Suffix(err error, suffix string) error {
	if err, ok := err.(*vtError); ok {
		err.err = fmt.Sprintf("%s%s", err.err, suffix)
	}
	return err
}
