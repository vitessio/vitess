// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqldb

import "fmt"

// SQLError is the error structure returned from calling a db library function
type SQLError struct {
	Num     int
	Message string
	Query   string
}

// NewSQLError returns a new SQLError
func NewSQLError(number int, format string, args ...interface{}) *SQLError {
	return &SQLError{Num: number, Message: fmt.Sprintf(format, args...)}
}

// Error implements the error interface
func (se *SQLError) Error() string {
	if se.Query == "" {
		return fmt.Sprintf("%v (errno %v)", se.Message, se.Num)
	}
	return fmt.Sprintf("%v (errno %v) during query: %s", se.Message, se.Num, se.Query)
}

// Number returns the internal mysql error code
func (se *SQLError) Number() int {
	return se.Num
}
