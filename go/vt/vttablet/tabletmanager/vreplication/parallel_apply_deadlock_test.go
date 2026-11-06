/*
Copyright 2026 The Vitess Authors.

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

package vreplication

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/sqlerror"
)

func TestIsLockWaitError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"lock wait timeout", sqlerror.NewSQLError(sqlerror.ERLockWaitTimeout, sqlerror.SSUnknownSQLState, "lock wait timeout"), true},
		{"deadlock", sqlerror.NewSQLError(sqlerror.ERLockDeadlock, sqlerror.SSUnknownSQLState, "deadlock"), true},
		{"other SQL error", sqlerror.NewSQLError(sqlerror.ERDupEntry, sqlerror.SSUnknownSQLState, "duplicate entry"), false},
		{"non-SQL error", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isLockWaitError(tt.err))
		})
	}
}
