// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqldb

import "fmt"

// SqlError is the error structure returned from calling a db library function
type SqlError struct {
	Num     int
	Message string
	Query   string
}

// NewSqlError returns a new SqlError
func NewSqlError(number int, format string, args ...interface{}) *SqlError {
	return &SqlError{Num: number, Message: fmt.Sprintf(format, args...)}
}

// Error implements the error interface
func (se *SqlError) Error() string {
	if se.Query == "" {
		return fmt.Sprintf("%v (errno %v)", se.Message, se.Num)
	}
	return fmt.Sprintf("%v (errno %v) during query: %s", se.Message, se.Num, se.Query)
}

// Number returns the internal mysql error code
func (se *SqlError) Number() int {
	return se.Num
}
