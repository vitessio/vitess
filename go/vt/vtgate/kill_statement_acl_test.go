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
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// TestKillStatementRejectsOtherConnections covers KILL and KILL QUERY from
// every MySQL entry that reaches Executor.handleKill. A caller may cancel
// only the connection that issued the statement. Another id is rejected even
// when that connection authenticated as the same MySQL user, and a kill that
// reads the target identity and looks the id up again must not act on the
// connection now stored under that id.
func TestKillStatementRejectsOtherConnections(t *testing.T) {
	oldAllow := allowKillStmt
	allowKillStmt = true
	t.Cleanup(func() { allowKillStmt = oldAllow })

	executor, _, _, _, _ := createExecutorEnv(t)
	vh := newVtgateHandler(newVTGate(executor, nil, nil, nil, nil))
	listener := newKillTestListener(t)

	// Listener connection ids start at 1 and increase by one. The cases below
	// allocate ids the same way so a kill of a low id hits another session.
	var nextID uint32 = 1
	allocID := func() uint32 {
		id := nextID
		nextID++
		return id
	}

	entries := []struct {
		name string
		olap bool
		call func(caller *mysql.Conn, sql string) error
	}{{
		name: "com_query",
		call: func(caller *mysql.Conn, sql string) error {
			return vh.ComQuery(caller, sql, func(*sqltypes.Result) error { return nil })
		},
	}, {
		name: "com_query_olap",
		olap: true,
		call: func(caller *mysql.Conn, sql string) error {
			return vh.ComQuery(caller, sql, func(*sqltypes.Result) error { return nil })
		},
	}, {
		name: "com_query_multi",
		call: func(caller *mysql.Conn, sql string) error {
			caller.Capabilities |= mysql.CapabilityClientMultiStatements
			return vh.ComQueryMulti(caller, sql, func(sqltypes.QueryResponse, bool, bool) error { return nil })
		},
	}, {
		name: "com_query_multi_olap",
		olap: true,
		call: func(caller *mysql.Conn, sql string) error {
			caller.Capabilities |= mysql.CapabilityClientMultiStatements
			return vh.ComQueryMulti(caller, sql, func(sqltypes.QueryResponse, bool, bool) error { return nil })
		},
	}, {
		name: "com_stmt_execute",
		call: func(caller *mysql.Conn, sql string) error {
			return vh.ComStmtExecute(caller, &mysql.PrepareData{
				PrepareStmt: sql,
				BindVars:    map[string]*querypb.BindVariable{},
			}, func(*sqltypes.Result) error { return nil })
		},
	}, {
		name: "com_stmt_execute_olap",
		olap: true,
		call: func(caller *mysql.Conn, sql string) error {
			return vh.ComStmtExecute(caller, &mysql.PrepareData{
				PrepareStmt: sql,
				BindVars:    map[string]*querypb.BindVariable{},
			}, func(*sqltypes.Result) error { return nil })
		},
	}}

	// "kill" and "kill connection" both take the KillConnection path.
	// "kill query" takes KillQuery. Both connection syntaxes are listed so a
	// parser split cannot hide one of them.
	statements := []string{"kill %d", "kill connection %d", "kill query %d"}
	targets := []struct {
		name string
		user string
	}{{
		name: "other_user",
		user: "other_user",
	}, {
		name: "same_user",
		user: "attacker",
	}}

	for _, entry := range entries {
		for _, statement := range statements {
			for _, target := range targets {
				t.Run(entry.name+"/"+statement+"/"+target.name, func(t *testing.T) {
					attackerID := allocID()
					targetID := allocID()
					attacker, _ := newWatchedConn(t, listener, attackerID, "attacker", &mysql.StaticUserData{Username: "attacker"})
					targetConn, targetCtx := newWatchedConn(t, listener, targetID, target.user, nil)
					// replacement is what a second lookup of targetID must not kill.
					// Its user matches the attacker, so a check that authorizes the
					// first object and then looks the id up again would let it through.
					replacement, replacementCtx := newWatchedConn(t, listener, targetID, "attacker", &mysql.StaticUserData{Username: "attacker"})
					targetConn.UserData = &swapOnGet{
						username: target.user,
						swap: func() {
							vh.connections[targetID] = replacement
						},
					}
					vh.NewConnection(attacker)
					vh.NewConnection(targetConn)
					setKillWorkload(vh, attacker, entry.olap)

					err := entry.call(attacker, fmt.Sprintf(statement, targetID))
					requireConnectionAlive(t, targetConn, targetCtx)
					requireConnectionAlive(t, replacement, replacementCtx)
					require.False(t, attacker.IsMarkedForClose())
					require.Error(t, err)
				})
			}
		}
	}
}

// TestKillStatementDoesNotFollowASecondLookup is the own-id case of the same
// bug. The statement names the caller's id, which is the id a check can
// authorize, and reading the caller identity swaps a different connection
// into that id before the kill runs. Cancelling the replacement means the
// connection acted on is not the connection that was authorized.
func TestKillStatementDoesNotFollowASecondLookup(t *testing.T) {
	oldAllow := allowKillStmt
	allowKillStmt = true
	t.Cleanup(func() { allowKillStmt = oldAllow })

	executor, _, _, _, _ := createExecutorEnv(t)
	vh := newVtgateHandler(newVTGate(executor, nil, nil, nil, nil))
	listener := newKillTestListener(t)

	var nextID uint32 = 1
	for _, statement := range []string{"kill %d", "kill connection %d", "kill query %d"} {
		t.Run(statement, func(t *testing.T) {
			id := nextID
			nextID++
			replacement, replacementCtx := newWatchedConn(t, listener, id, "attacker", &mysql.StaticUserData{Username: "attacker"})
			attacker, _ := newWatchedConn(t, listener, id, "attacker", &swapOnGet{
				username: "attacker",
				swap: func() {
					vh.connections[id] = replacement
				},
			})
			vh.NewConnection(attacker)

			_ = vh.ComQuery(attacker, fmt.Sprintf(statement, id), func(*sqltypes.Result) error { return nil })
			// The replacement was stored under the caller's id after the caller
			// identity was read. It is not the connection that issued the kill.
			requireConnectionAlive(t, replacement, replacementCtx)
		})
	}
}

// TestKillStatementStillCancelsCallerConnection keeps the allowed operation:
// with allow-kill-statement on, a connection can still kill its own id.
func TestKillStatementStillCancelsCallerConnection(t *testing.T) {
	oldAllow := allowKillStmt
	allowKillStmt = true
	t.Cleanup(func() { allowKillStmt = oldAllow })

	executor, _, _, _, _ := createExecutorEnv(t)
	vh := newVtgateHandler(newVTGate(executor, nil, nil, nil, nil))
	listener := newKillTestListener(t)

	t.Run("kill", func(t *testing.T) {
		probe := &cancelProbe{username: "attacker"}
		caller, _ := newWatchedConn(t, listener, 1, "attacker", probe)
		probe.conn = caller
		bystander, bystanderCtx := newWatchedConn(t, listener, 2, "other_user", &mysql.StaticUserData{Username: "other_user"})
		vh.NewConnection(caller)
		vh.NewConnection(bystander)

		err := vh.ComQuery(caller, "kill 1", func(*sqltypes.Result) error { return nil })
		require.NoError(t, err)
		require.True(t, probe.cancelled)
		require.True(t, caller.IsMarkedForClose())
		requireConnectionAlive(t, bystander, bystanderCtx)
	})

	t.Run("kill_query", func(t *testing.T) {
		probe := &cancelProbe{username: "attacker"}
		caller, _ := newWatchedConn(t, listener, 3, "attacker", probe)
		probe.conn = caller
		bystander, bystanderCtx := newWatchedConn(t, listener, 4, "other_user", &mysql.StaticUserData{Username: "other_user"})
		vh.NewConnection(caller)
		vh.NewConnection(bystander)

		err := vh.ComQuery(caller, "kill query 3", func(*sqltypes.Result) error { return nil })
		require.NoError(t, err)
		require.True(t, probe.cancelled)
		require.False(t, caller.IsMarkedForClose())
		requireConnectionAlive(t, bystander, bystanderCtx)
	})
}

func newKillTestListener(t *testing.T) *mysql.Listener {
	t.Helper()
	listener, err := mysql.NewListener("tcp", "127.0.0.1:", mysql.NewAuthServerNone(), &testHandler{}, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	t.Cleanup(listener.Close)
	return listener
}

func newWatchedConn(t *testing.T, listener *mysql.Listener, id uint32, user string, data mysql.Getter) (*mysql.Conn, context.Context) {
	t.Helper()
	conn := mysql.GetTestServerConn(listener)
	conn.ConnectionID = id
	conn.User = user
	if data == nil {
		data = &mysql.StaticUserData{Username: user}
	}
	conn.UserData = data
	ctx, cancel := context.WithCancel(t.Context())
	conn.UpdateCancelCtx(cancel)
	t.Cleanup(cancel)
	return conn, ctx
}

func setKillWorkload(vh *vtgateHandler, conn *mysql.Conn, olap bool) {
	workload := querypb.ExecuteOptions_OLTP
	if olap {
		workload = querypb.ExecuteOptions_OLAP
	}
	vh.session(conn).Options.Workload = workload
}

func requireConnectionAlive(t *testing.T, conn *mysql.Conn, ctx context.Context) {
	t.Helper()
	require.NoError(t, ctx.Err())
	require.False(t, conn.IsMarkedForClose())
}

// swapOnGet replaces the map entry for a connection id the first time the
// server reads that connection's caller identity. The username it returns is
// the identity of the connection that was stored there at the check.
type swapOnGet struct {
	username string
	swap     func()
	once     sync.Once
}

func (s *swapOnGet) Get() *querypb.VTGateCallerID {
	s.once.Do(s.swap)
	return &querypb.VTGateCallerID{Username: s.username}
}

// cancelProbe records CancelCtx. ComQuery installs its own cancel before it
// reads the caller identity, so the probe replaces that cancel from Get and
// the test can see a kill of this connection.
type cancelProbe struct {
	username  string
	conn      *mysql.Conn
	cancelled bool
}

func (p *cancelProbe) Get() *querypb.VTGateCallerID {
	p.conn.UpdateCancelCtx(func() { p.cancelled = true })
	return &querypb.VTGateCallerID{Username: p.username}
}
