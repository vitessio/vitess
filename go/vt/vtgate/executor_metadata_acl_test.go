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

package vtgate

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/callerid"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"
)

const (
	// authPlaceholder is not a credential. The static auth server needs a
	// password to finish the MySQL handshake.
	authPlaceholder = "placeholder"

	metadataMarkerOne = "marker-one"
	metadataMarkerTwo = "marker-two"
	metadataKeyOne    = "aclprobe"
	metadataKeyTwo    = "otherprobe"
)

// TestShowVitessMetadataRequiresSameACLAsSet stores cluster metadata as a
// caller listed in vschema-ddl-authorized-users, then requires every other
// caller to be rejected on read. SET already checks the immediate caller
// (MySQL auth UserData, or the gRPC certificate / unauthenticated fallback).
// A MySQL login name and a client-supplied effective caller id are not that
// identity. An absent caller is denied.
func TestShowVitessMetadataRequiresSameACLAsSet(t *testing.T) {
	vschemaacl.AuthorizedDDLUsers.Set(vschemaacl.NewAuthorizedDDLUsers("operator"))
	t.Cleanup(func() {
		vschemaacl.AuthorizedDDLUsers.Set(vschemaacl.NewAuthorizedDDLUsers(""))
	})

	executor, _, _, _, _ := createExecutorEnv(t)
	gate := newVTGate(executor, nil, nil, nil, nil)
	handler := newVtgateHandler(gate)

	authJSON := `{
		"vitess-operator":[{"Password":"` + authPlaceholder + `","UserData":"operator"}],
		"reader":[{"Password":"` + authPlaceholder + `","UserData":"reader"}],
		"operator":[{"Password":"` + authPlaceholder + `","UserData":"reader"}],
		"plain":[{"Password":"` + authPlaceholder + `","UserData":""}]
	}`
	// file is empty, so the auth server does not start a reload goroutine.
	authServer := mysql.NewAuthServerStatic("", authJSON, 0)

	listener, err := mysql.NewListener("tcp", "127.0.0.1:", authServer, handler, 0, 0, false, false, 0, 0, true)
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	go listener.Accept()

	tcpAddr := listener.Addr().(*net.TCPAddr)
	dial := func(t *testing.T, login string, multi bool) *mysql.Conn {
		t.Helper()
		var conn *mysql.Conn
		require.Eventually(t, func() bool {
			c, dialErr := mysql.Connect(t.Context(), &mysql.ConnParams{
				Host:                  tcpAddr.IP.String(),
				Port:                  tcpAddr.Port,
				Uname:                 login,
				Pass:                  authPlaceholder,
				EnableMultiStatements: multi,
			})
			if dialErr != nil {
				return false
			}
			conn = c
			return true
		}, 5*time.Second, 10*time.Millisecond)
		t.Cleanup(func() { conn.Close() })
		return conn
	}

	showAll := "show vitess_metadata variables"
	showOne := "show vitess_metadata variables like '" + metadataKeyOne + "'"
	showAny := "show vitess_metadata variables like '%'"
	shows := []string{showAll, showOne, showAny}

	operator := dial(t, "vitess-operator", false)
	_, err = operator.ExecuteFetch("set @@vitess_metadata."+metadataKeyOne+" = '"+metadataMarkerOne+"'", 1000, false)
	require.NoError(t, err)
	_, err = operator.ExecuteFetch("set @@vitess_metadata."+metadataKeyTwo+" = '"+metadataMarkerTwo+"'", 1000, false)
	require.NoError(t, err)

	t.Run("authorized read", func(t *testing.T) {
		qr, err := operator.ExecuteFetch(showAll, 1000, true)
		require.NoError(t, err)
		require.True(t, metadataRowsContain(qr, metadataKeyOne, metadataMarkerOne))
		require.True(t, metadataRowsContain(qr, metadataKeyTwo, metadataMarkerTwo))

		qr, err = operator.ExecuteFetch(showOne, 1000, true)
		require.NoError(t, err)
		require.True(t, metadataRowsContain(qr, metadataKeyOne, metadataMarkerOne))
		require.False(t, metadataRowsContain(qr, metadataKeyTwo, metadataMarkerTwo))
	})

	t.Run("unauthorized set", func(t *testing.T) {
		reader := dial(t, "reader", false)
		_, err := reader.ExecuteFetch("set @@vitess_metadata."+metadataKeyOne+" = 'x'", 1000, false)
		requireMetadataDeniedMySQL(t, "reader", nil, err)

		// Login name is on the ACL. UserData, the immediate caller, is not.
		spoofedLogin := dial(t, "operator", false)
		_, err = spoofedLogin.ExecuteFetch("set @@vitess_metadata."+metadataKeyOne+" = 'x'", 1000, false)
		requireMetadataDeniedMySQL(t, "reader", nil, err)
	})

	for _, login := range []struct {
		name      string
		immediate string
	}{
		{name: "reader", immediate: "reader"},
		{name: "operator", immediate: "reader"},
		{name: "plain", immediate: ""},
	} {
		t.Run("mysql show "+login.name, func(t *testing.T) {
			conn := dial(t, login.name, false)
			for _, query := range shows {
				qr, err := conn.ExecuteFetch(query, 1000, true)
				requireMetadataDeniedMySQL(t, login.immediate, qr, err)
			}
		})
	}

	t.Run("mysql olap stream", func(t *testing.T) {
		conn := dial(t, "reader", false)
		_, err := conn.ExecuteFetch("set workload = olap", 1000, false)
		require.NoError(t, err)
		qr, err := conn.ExecuteFetch(showAll, 1000, true)
		requireMetadataDeniedMySQL(t, "reader", qr, err)
	})

	t.Run("mysql multi statement", func(t *testing.T) {
		conn := dial(t, "reader", true)
		results, err := fetchMulti(conn, showAll+"; "+showOne)
		for _, qr := range results {
			requireNoMetadata(t, qr)
		}
		requireMetadataDeniedMySQL(t, "reader", nil, err)
	})

	t.Run("mysql prepare", func(t *testing.T) {
		c := mysql.GetTestServerConn(listener)
		c.User = "operator"
		c.UserData = &mysql.StaticUserData{Username: "reader"}
		_, _, err := handler.ComPrepare(c, showAll)
		requireMetadataDeniedMySQL(t, "reader", nil, err)
	})

	t.Run("mysql prepared execute", func(t *testing.T) {
		c := mysql.GetTestServerConn(listener)
		c.User = "operator"
		c.UserData = &mysql.StaticUserData{Username: "reader"}
		err := handler.ComStmtExecute(c, &mysql.PrepareData{PrepareStmt: showAll}, func(qr *sqltypes.Result) error {
			requireNoMetadata(t, qr)
			return nil
		})
		requireMetadataDeniedMySQL(t, "reader", nil, err)
	})

	t.Run("mysql prepared stream execute", func(t *testing.T) {
		c := mysql.GetTestServerConn(listener)
		c.User = "operator"
		c.UserData = &mysql.StaticUserData{Username: "reader"}
		handler.session(c).Options.Workload = querypb.ExecuteOptions_OLAP
		err := handler.ComStmtExecute(c, &mysql.PrepareData{PrepareStmt: showOne}, func(qr *sqltypes.Result) error {
			requireNoMetadata(t, qr)
			return nil
		})
		requireMetadataDeniedMySQL(t, "reader", nil, err)
	})

	t.Run("absent caller", func(t *testing.T) {
		fresh := &vtgatepb.Session{Autocommit: true}
		_, qr, err := gate.Execute(t.Context(), nil, fresh, showAll, nil, false)
		require.Equal(t, vtrpcpb.Code_PERMISSION_DENIED, vterrors.Code(err))
		require.ErrorContains(t, err, metadataDeniedMessage(""))
		requireNoMetadata(t, qr)

		_, err = gate.StreamExecute(t.Context(), nil, fresh, showOne, nil, false, func(qr *sqltypes.Result) error {
			requireNoMetadata(t, qr)
			return nil
		})
		require.Equal(t, vtrpcpb.Code_PERMISSION_DENIED, vterrors.Code(err))
		require.ErrorContains(t, err, metadataDeniedMessage(""))
	})

	t.Run("allow all still reads", func(t *testing.T) {
		vschemaacl.AuthorizedDDLUsers.Set(vschemaacl.NewAuthorizedDDLUsers("%"))
		t.Cleanup(func() {
			vschemaacl.AuthorizedDDLUsers.Set(vschemaacl.NewAuthorizedDDLUsers("operator"))
		})
		reader := dial(t, "reader", false)
		qr, err := reader.ExecuteFetch(showAll, 1000, true)
		require.NoError(t, err)
		require.True(t, metadataRowsContain(qr, metadataKeyOne, metadataMarkerOne))
		require.True(t, metadataRowsContain(qr, metadataKeyTwo, metadataMarkerTwo))
	})

	t.Run("empty acl denies the writer", func(t *testing.T) {
		vschemaacl.AuthorizedDDLUsers.Set(vschemaacl.NewAuthorizedDDLUsers(""))
		t.Cleanup(func() {
			vschemaacl.AuthorizedDDLUsers.Set(vschemaacl.NewAuthorizedDDLUsers("operator"))
		})
		qr, err := operator.ExecuteFetch(showAll, 1000, true)
		requireMetadataDeniedMySQL(t, "operator", qr, err)

		ctx := callerid.NewContext(t.Context(), nil, callerid.NewImmediateCallerID("operator"))
		_, qr, err = gate.Execute(ctx, nil, &vtgatepb.Session{Autocommit: true}, showAll, nil, false)
		require.Equal(t, vtrpcpb.Code_PERMISSION_DENIED, vterrors.Code(err))
		require.ErrorContains(t, err, metadataDeniedMessage("operator"))
		requireNoMetadata(t, qr)
	})
}

func fetchMulti(conn *mysql.Conn, query string) ([]*sqltypes.Result, error) {
	qr, more, err := conn.ExecuteFetchMulti(query, 1000, true)
	if err != nil {
		return nil, err
	}
	results := []*sqltypes.Result{qr}
	for more {
		var next *sqltypes.Result
		next, more, _, err = conn.ReadQueryResult(1000, true)
		if next != nil {
			results = append(results, next)
		}
		if err != nil {
			return results, err
		}
	}
	return results, nil
}

func requireMetadataDeniedMySQL(t *testing.T, user string, qr *sqltypes.Result, err error) {
	t.Helper()
	requireNoMetadata(t, qr)
	require.Error(t, err)
	var sqlErr *sqlerror.SQLError
	require.ErrorAs(t, err, &sqlErr)
	require.Equal(t, sqlerror.ERAccessDeniedError, sqlErr.Num)
	require.Contains(t, sqlErr.Message, metadataDeniedMessage(user))
}

func metadataDeniedMessage(user string) string {
	return fmt.Sprintf("User '%s' not authorized to perform vitess metadata operations", user)
}

func metadataRowsContain(qr *sqltypes.Result, key, value string) bool {
	if qr == nil {
		return false
	}
	for _, row := range qr.Rows {
		var keySeen, valueSeen bool
		for _, col := range row {
			text := col.ToString()
			if text == key {
				keySeen = true
			}
			if text == value {
				valueSeen = true
			}
		}
		if keySeen && valueSeen {
			return true
		}
	}
	return false
}

func requireNoMetadata(t *testing.T, qr *sqltypes.Result) {
	t.Helper()
	if qr == nil {
		return
	}
	for _, row := range qr.Rows {
		for _, col := range row {
			text := col.ToString()
			require.NotEqual(t, metadataMarkerOne, text)
			require.NotEqual(t, metadataMarkerTwo, text)
			require.NotEqual(t, metadataKeyOne, text)
			require.NotEqual(t, metadataKeyTwo, text)
		}
	}
}
