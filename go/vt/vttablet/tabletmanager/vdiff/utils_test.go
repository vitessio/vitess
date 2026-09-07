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

package vdiff

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/sqlerror"
)

// TestErrWithoutQueryEcho confirms that the echoed failing statement (which can
// be arbitrarily large, e.g. when it embeds a big VDiff report) is dropped while
// the actionable message/errno/sqlstate are preserved. This is what keeps a
// persisted error small enough that the recording statement does not itself
// exceed max_allowed_packet.
func TestErrWithoutQueryEcho(t *testing.T) {
	huge := strings.Repeat("x", 200000)
	sqlErr := &sqlerror.SQLError{
		Num:     sqlerror.ERNetPacketTooLarge,
		State:   "08S01",
		Message: "Got a packet bigger than 'max_allowed_packet' bytes",
		Query:   "update _vt.vdiff_table set report = '" + huge + "'",
	}
	// Sanity: the raw SQL error echoes the (huge) failing query.
	require.ErrorContains(t, sqlErr, huge)

	got := errWithoutQueryEcho(sqlErr)
	require.ErrorContains(t, got, "Got a packet bigger than 'max_allowed_packet' bytes")
	require.ErrorContains(t, got, "(errno 1153)")
	require.ErrorContains(t, got, "(sqlstate 08S01)")
	require.NotContains(t, got.Error(), huge, "the query echo (payload) must not be carried in the error")

	// A SQL error without a query echo is returned unchanged.
	noEcho := &sqlerror.SQLError{Num: sqlerror.ERNetPacketTooLarge, State: "08S01", Message: "boom"}
	require.Same(t, noEcho, errWithoutQueryEcho(noEcho))

	// Non-SQL errors pass through unchanged.
	plain := errors.New("plain error")
	require.Equal(t, plain, errWithoutQueryEcho(plain))
}
