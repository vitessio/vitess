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

package grpc_api_sha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/callerid"
)

// TestEffectiveCallerIDWithAccess verifies that an authenticated gRPC SHA1 user with an effectiveCallerID that has ACL access can execute queries
func TestEffectiveCallerIDWithAccess(t *testing.T) {
	vtgateConn, err := cluster.DialVTGate(t.Context(), t.Name(), vtgateGrpcAddress, "some_other_user", "test_password")
	require.NoError(t, err)
	defer vtgateConn.Close()

	session := vtgateConn.Session(keyspaceName+"@primary", nil)
	query := "SELECT id FROM test_table"
	ctx := callerid.NewContext(t.Context(), callerid.NewEffectiveCallerID("user_with_access", "", ""), nil)
	_, err = session.Execute(ctx, query, nil, false)
	assert.NoError(t, err)
}

// TestEffectiveCallerIDWithNoAccess verifies that an authenticated gRPC SHA1 user without an effectiveCallerID that has ACL access cannot execute queries
func TestEffectiveCallerIDWithNoAccess(t *testing.T) {
	vtgateConn, err := cluster.DialVTGate(t.Context(), t.Name(), vtgateGrpcAddress, "another_unrelated_user", "test_password")
	require.NoError(t, err)
	defer vtgateConn.Close()

	session := vtgateConn.Session(keyspaceName+"@primary", nil)
	query := "SELECT id FROM test_table"
	ctx := callerid.NewContext(t.Context(), callerid.NewEffectiveCallerID("user_no_access", "", ""), nil)
	_, err = session.Execute(ctx, query, nil, false)
	require.Error(t, err)
	assert.ErrorContains(t, err, "Select command denied to user")
	assert.ErrorContains(t, err, "for table 'test_table' (ACL check error)")
}

// TestAuthenticatedUserWithAccess verifies that an authenticated gRPC SHA1 user with ACL access can execute queries
func TestAuthenticatedUserWithAccess(t *testing.T) {
	vtgateConn, err := cluster.DialVTGate(t.Context(), t.Name(), vtgateGrpcAddress, "user_with_access", "test_password")
	require.NoError(t, err)
	defer vtgateConn.Close()

	session := vtgateConn.Session(keyspaceName+"@primary", nil)
	query := "SELECT id FROM test_table"
	_, err = session.Execute(t.Context(), query, nil, false)
	assert.NoError(t, err)
}

// TestAuthenticatedUserNoAccess verifies that an authenticated gRPC SHA1 user with no ACL access cannot execute queries
func TestAuthenticatedUserNoAccess(t *testing.T) {
	vtgateConn, err := cluster.DialVTGate(t.Context(), t.Name(), vtgateGrpcAddress, "user_no_access", "test_password")
	require.NoError(t, err)
	defer vtgateConn.Close()

	session := vtgateConn.Session(keyspaceName+"@primary", nil)
	query := "SELECT id FROM test_table"
	_, err = session.Execute(t.Context(), query, nil, false)
	require.Error(t, err)
	assert.ErrorContains(t, err, "Select command denied to user")
	assert.ErrorContains(t, err, "for table 'test_table' (ACL check error)")
}

// TestUnauthenticatedUser verifies that an unauthenticated gRPC user cannot execute queries
func TestUnauthenticatedUser(t *testing.T) {
	vtgateConn, err := cluster.DialVTGate(t.Context(), t.Name(), vtgateGrpcAddress, "", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	session := vtgateConn.Session(keyspaceName+"@primary", nil)
	query := "SELECT id FROM test_table"
	_, err = session.Execute(t.Context(), query, nil, false)
	require.Error(t, err)
	assert.ErrorContains(t, err, "invalid credentials")
}

// TestWrongPassword verifies that providing the wrong password fails authentication
func TestWrongPassword(t *testing.T) {
	vtgateConn, err := cluster.DialVTGate(t.Context(), t.Name(), vtgateGrpcAddress, "user_with_access", "wrong_password")
	require.NoError(t, err)
	defer vtgateConn.Close()

	session := vtgateConn.Session(keyspaceName+"@primary", nil)
	query := "SELECT id FROM test_table"
	_, err = session.Execute(t.Context(), query, nil, false)
	require.Error(t, err)
	assert.ErrorContains(t, err, "invalid credentials")
}

// TestNonExistentUser verifies that a non-existent user cannot authenticate
func TestNonExistentUser(t *testing.T) {
	vtgateConn, err := cluster.DialVTGate(t.Context(), t.Name(), vtgateGrpcAddress, "nonexistent_user", "test_password")
	require.NoError(t, err)
	defer vtgateConn.Close()

	session := vtgateConn.Session(keyspaceName+"@primary", nil)
	query := "SELECT id FROM test_table"
	_, err = session.Execute(t.Context(), query, nil, false)
	require.Error(t, err)
	assert.ErrorContains(t, err, "invalid credentials")
}
