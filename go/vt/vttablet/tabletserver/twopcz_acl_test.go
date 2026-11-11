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

package tabletserver

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	// Register the linkname targets below. The check is the real policy.
	_ "unsafe"
)

// The policy the handler consults is process-global and fixed on first use.
// Tests install the real registered policy after that, then put the previous
// one back.
//
//go:linkname aclCurrentPolicy vitess.io/vitess/go/acl.currentPolicy
var aclCurrentPolicy acl.Policy

//go:linkname aclPolicies vitess.io/vitess/go/acl.policies
var aclPolicies map[string]acl.Policy

// TestTwopczResolutionRequiresAdminPost is the spec for /twopcz.
//
// A caller the read-only policy accepts (debugging, not admin) must be able to
// list in-flight transactions and must not be able to discard, roll back,
// commit, or conclude one. The same actions require POST even when the
// default allow-all policy grants admin. The request names two dtids; only
// the one FormValue returns may change.
//
// These checks fail while the handler runs the actions after a debugging check.
func TestTwopczResolutionRequiresAdminPost(t *testing.T) {
	db := setUpQueryExecutorTest(t)
	registerTwopczActionQueries(db)
	tsv := newTestTabletServer(t.Context(), 0, db)
	t.Cleanup(func() {
		db.Close()
		tsv.StopService()
	})
	require.True(t, tsv.te.twopcEnabled, "two-phase commit is on when twopc-abandon-age is positive")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	serveErr := make(chan error, 1)
	go func() { serveErr <- servenv.HTTPServe(ln) }()
	t.Cleanup(func() {
		ln.Close()
		require.NoError(t, <-serveErr)
	})
	addr := ln.Addr().String()

	// savePolicy runs once and would clobber a policy installed first.
	_ = acl.CheckAccessHTTP(httptest.NewRequest(http.MethodGet, "/twopcz", nil), acl.DEBUGGING)

	actions := []string{"Discard", "Rollback", "Commit", "Conclude"}
	places := []formPlace{formQuery, formBody}

	t.Run("read-only", func(t *testing.T) {
		useACLPolicy(t, "read-only")
		probe := httptest.NewRequest(http.MethodGet, "/twopcz", nil)
		require.EqualError(t, acl.CheckAccessHTTP(probe, acl.ADMIN), "not allowed: read-only security-policy enforced")
		require.NoError(t, acl.CheckAccessHTTP(probe, acl.DEBUGGING))

		for _, action := range actions {
			for _, method := range []string{http.MethodGet, http.MethodPost} {
				for _, place := range places {
					if method == http.MethodGet && place == formBody {
						continue
					}
					t.Run(method+" "+place.String()+" "+action, func(t *testing.T) {
						assertTwopczAction(t, tsv, db, addr, twopczCall{
							method: method,
							place:  place,
							action: action,
							allow:  false,
						})
					})
				}
			}
		}
		t.Run("json get commit", func(t *testing.T) {
			assertTwopczAction(t, tsv, db, addr, twopczCall{
				method: http.MethodGet,
				place:  formQuery,
				action: "Commit",
				format: "json",
				allow:  false,
			})
		})
		t.Run("list", func(t *testing.T) {
			assertTwopczAction(t, tsv, db, addr, twopczCall{
				method: http.MethodGet,
				place:  formQuery,
				allow:  false,
				list:   true,
			})
		})
	})

	t.Run("default", func(t *testing.T) {
		useACLPolicy(t, "")
		probe := httptest.NewRequest(http.MethodGet, "/twopcz", nil)
		require.NoError(t, acl.CheckAccessHTTP(probe, acl.ADMIN))
		require.NoError(t, acl.CheckAccessHTTP(probe, acl.DEBUGGING))

		for _, action := range actions {
			t.Run("GET "+action, func(t *testing.T) {
				assertTwopczAction(t, tsv, db, addr, twopczCall{
					method: http.MethodGet,
					place:  formQuery,
					action: action,
					allow:  false,
				})
			})
			t.Run("POST "+action, func(t *testing.T) {
				assertTwopczAction(t, tsv, db, addr, twopczCall{
					method: http.MethodPost,
					place:  formQuery,
					action: action,
					allow:  true,
				})
			})
		}
		t.Run("POST body commit", func(t *testing.T) {
			assertTwopczAction(t, tsv, db, addr, twopczCall{
				method: http.MethodPost,
				place:  formBody,
				action: "Commit",
				allow:  true,
			})
		})
	})
}

type formPlace int

const (
	formQuery formPlace = iota
	formBody
)

func (p formPlace) String() string {
	if p == formBody {
		return "body"
	}
	return "query"
}

type twopczCall struct {
	method string
	place  formPlace
	action string
	format string
	allow  bool
	list   bool
}

func assertTwopczAction(t *testing.T, tsv *TabletServer, db *fakesqldb.DB, addr string, call twopczCall) {
	t.Helper()
	aa, bb := prepareTwopczPair(t, tsv)
	aaID := uint32(aa.dbConn.Conn.ID())
	bbID := uint32(bb.dbConn.Conn.ID())
	before := twopczDeleteCounts(db)
	db.ResetQueryLog()

	status, body := callTwopcz(t, addr, call)

	deletes := twopczDeleteCounts(db)
	commits := db.QueryConnIDs("commit")
	rollbacks := db.QueryConnIDs("rollback")
	detail := func() string {
		return fmt.Sprintf("status=%d completed=%t log=%s", status, strings.Contains(body, "completed"), db.QueryLog())
	}

	// The bystander dtid is the second form value. Nothing may commit, roll
	// back, or delete it: a check that then acts on every dtid in the form fails.
	require.Same(t, bb, preparedLocked(tsv, "bb"), "bystander dtid bb was taken out of the prepared pool (%s)", detail())
	require.True(t, bb.IsInTransaction(), "bystander dtid bb was committed or rolled back (%s)", detail())
	require.False(t, connTouched(commits, rollbacks, bbID), "bystander connection was committed or rolled back (%s)", detail())
	require.Equal(t, before.bb, deletes.bb, "bystander dtid bb was deleted (%s)", detail())

	if call.list {
		require.Equal(t, http.StatusOK, status, detail())
		require.Contains(t, body, "Prepared Transactions", detail())
		require.NotContains(t, body, "Access denied", detail())
		require.Same(t, aa, preparedLocked(tsv, "aa"), detail())
		require.True(t, aa.IsInTransaction(), detail())
		require.Equal(t, before.aa, deletes.aa, detail())
		require.False(t, connTouched(commits, rollbacks, aaID), detail())
		return
	}

	if !call.allow {
		// State first: a refusal that still commits, rolls back, or deletes fails here.
		require.Same(t, aa, preparedLocked(tsv, "aa"), "dtid aa left the prepared pool (%s)", detail())
		require.True(t, aa.IsInTransaction(), "dtid aa was committed or rolled back (%s)", detail())
		require.False(t, connTouched(commits, rollbacks, aaID), "dtid aa connection was committed or rolled back (%s)", detail())
		require.Equal(t, before.aa, deletes.aa, "dtid aa metadata was deleted (%s)", detail())
		require.GreaterOrEqual(t, status, 400, "action %s was accepted (%s)", call.action, detail())
		require.NotContains(t, body, "completed", detail())
		return
	}

	require.Less(t, status, 400, detail())
	require.Contains(t, body, call.action+"(aa): completed.", detail())
	require.NotContains(t, body, call.action+"(bb)", detail())
	switch call.action {
	case "Commit":
		require.Nil(t, preparedLocked(tsv, "aa"), "prepared transaction aa was not committed (%s)", detail())
		require.Contains(t, commits, aaID, "commit did not run on the prepared connection (%s)", detail())
		require.NotContains(t, rollbacks, aaID, detail())
		require.Equal(t, before.aa.redoState+1, deletes.aa.redoState, detail())
		require.Equal(t, before.aa.redoStmt+1, deletes.aa.redoStmt, detail())
		require.Equal(t, before.aa.dtState, deletes.aa.dtState, detail())
		require.Equal(t, before.aa.dtPart, deletes.aa.dtPart, detail())
	case "Rollback", "Discard":
		require.Nil(t, preparedLocked(tsv, "aa"), "prepared transaction aa was not rolled back (%s)", detail())
		require.Contains(t, rollbacks, aaID, "rollback did not run on the prepared connection (%s)", detail())
		require.NotContains(t, commits, aaID, detail())
		require.Equal(t, before.aa.redoState+1, deletes.aa.redoState, detail())
		require.Equal(t, before.aa.redoStmt+1, deletes.aa.redoStmt, detail())
		require.Equal(t, before.aa.dtState, deletes.aa.dtState, detail())
		require.Equal(t, before.aa.dtPart, deletes.aa.dtPart, detail())
	case "Conclude":
		require.Same(t, aa, preparedLocked(tsv, "aa"), "conclude rolled back or committed the prepared transaction (%s)", detail())
		require.True(t, aa.IsInTransaction(), detail())
		require.False(t, connTouched(commits, rollbacks, aaID), detail())
		require.Equal(t, before.aa.redoState, deletes.aa.redoState, detail())
		require.Equal(t, before.aa.redoStmt, deletes.aa.redoStmt, detail())
		require.Equal(t, before.aa.dtState+1, deletes.aa.dtState, detail())
		require.Equal(t, before.aa.dtPart+1, deletes.aa.dtPart, detail())
	default:
		t.Fatalf("unknown action %s", call.action)
	}
}

func useACLPolicy(t *testing.T, name string) {
	t.Helper()
	prev := aclCurrentPolicy
	t.Cleanup(func() { aclCurrentPolicy = prev })
	if name == "" {
		aclCurrentPolicy = nil
		return
	}
	policy, ok := aclPolicies[name]
	require.Truef(t, ok, "security policy %q is not registered", name)
	aclCurrentPolicy = policy
}

func prepareTwopczPair(t *testing.T, tsv *TabletServer) (aa, bb *StatefulConnection) {
	t.Helper()
	txe := newTwopczExec(t, tsv)
	// A previous case may have left these dtids prepared.
	_ = txe.RollbackPrepared("aa", 0)
	_ = txe.RollbackPrepared("bb", 0)
	require.NoError(t, txe.Prepare(newTxForPrep(t.Context(), tsv), "aa"))
	require.NoError(t, txe.Prepare(newTxForPrep(t.Context(), tsv), "bb"))
	aa = preparedLocked(tsv, "aa")
	bb = preparedLocked(tsv, "bb")
	require.NotNil(t, aa, "dtid aa was not prepared")
	require.NotNil(t, bb, "dtid bb was not prepared")
	require.True(t, aa.IsUnixSocket())
	require.True(t, aa.IsInTransaction())
	require.True(t, bb.IsInTransaction())
	return aa, bb
}

func newTwopczExec(t *testing.T, tsv *TabletServer) *DTExecutor {
	t.Helper()
	ctx := t.Context()
	return NewDTExecutor(ctx, tabletenv.NewLogStats(ctx, "twopcz-test", streamlog.NewQueryLogConfigForTest()), tsv.te, tsv.qe, tsv.getShard)
}

func preparedLocked(tsv *TabletServer, dtid string) *StatefulConnection {
	pp := tsv.te.preparedPool
	pp.mu.Lock()
	defer pp.mu.Unlock()
	return pp.conns[dtid]
}

type deleteCounts struct {
	redoState int
	redoStmt  int
	dtState   int
	dtPart    int
}

type pairDeletes struct {
	aa deleteCounts
	bb deleteCounts
}

func twopczDeleteCounts(db *fakesqldb.DB) pairDeletes {
	count := func(dtid, table string) int {
		return db.GetQueryCalledNum("delete from _vt." + table + " where dtid = _binary'" + dtid + "'")
	}
	one := func(dtid string) deleteCounts {
		return deleteCounts{
			redoState: count(dtid, "redo_state"),
			redoStmt:  count(dtid, "redo_statement"),
			dtState:   count(dtid, "dt_state"),
			dtPart:    count(dtid, "dt_participant"),
		}
	}
	return pairDeletes{aa: one("aa"), bb: one("bb")}
}

func connTouched(commits, rollbacks []uint32, id uint32) bool {
	return slices.Contains(commits, id) || slices.Contains(rollbacks, id)
}

func callTwopcz(t *testing.T, addr string, call twopczCall) (int, string) {
	t.Helper()
	form := url.Values{}
	// Two values: the handler must act on the first (aa) only.
	form.Add("dtid", "aa")
	form.Add("dtid", "bb")
	if call.action != "" {
		form.Set("Action", call.action)
	}
	if call.format != "" {
		form.Set("format", call.format)
	}
	endpoint := "http://" + addr + "/TabletServerTest/twopcz"
	var body io.Reader
	if call.place == formBody {
		body = strings.NewReader(form.Encode())
	} else if len(form) > 0 {
		endpoint += "?" + form.Encode()
	}
	req, err := http.NewRequest(call.method, endpoint, body)
	require.NoError(t, err)
	if call.place == formBody {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	req = req.WithContext(t.Context())
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NotEqual(t, http.StatusNotFound, resp.StatusCode, "registered /twopcz handler was not reached: %s", payload)
	return resp.StatusCode, string(payload)
}

func registerTwopczActionQueries(db *fakesqldb.DB) {
	db.AddQuery("update test_table set `name` = 2 where pk = 1 limit 10001", &sqltypes.Result{})
	db.AddQueryPattern(`insert into _vt\.redo_state\(dtid, state, time_created\) values \(_binary'(aa|bb)', 1,.*`, &sqltypes.Result{})
	db.AddQueryPattern(`insert into _vt\.redo_statement.*`, &sqltypes.Result{})
	db.AddQueryPattern(`select t\.dtid, t\.state, t\.time_created, s\.statement.*`, &sqltypes.Result{})
	db.AddQueryPattern(`select t\.dtid, t\.state, t\.time_created, p\.keyspace, p\.shard.*`, &sqltypes.Result{})
	for _, dtid := range []string{"aa", "bb"} {
		for _, table := range []string{"redo_state", "redo_statement", "dt_state", "dt_participant"} {
			db.AddQuery("delete from _vt."+table+" where dtid = _binary'"+dtid+"'", &sqltypes.Result{})
		}
	}
}
