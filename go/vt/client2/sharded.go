// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package client2

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"time"

	// FIXME(msolomon) needed for the field mapping. Probably should be part of
	// tablet, or moved.
	mproto "code.google.com/p/vitess/go/mysql/proto"
	"code.google.com/p/vitess/go/vt/client2/tablet"
	// FIXME(msolomon) zk indirect dependency
	"code.google.com/p/vitess/go/vt/naming"
	// FIXME(msolomon) seems like a subpackage
	"code.google.com/p/vitess/go/vt/sqlparser"
	"code.google.com/p/vitess/go/zk"
)

// The sharded client handles writing to multiple shards across the
// database.  Where possible, this is compatible with the Go database
// driver, but that requires using the sqlparser. Ideally that should
// be a separate client layers on top of the basic shard-aware client.
//
// The ShardedConn can handles several separate aspects:
//  * loading/reloading tablet addresses on demand from zk/zkocc
//  * maintaining at most one connection to each tablet as required
//  * transaction tracking across shards
//  * preflight checking all transactions before attempting to commit
//    (reduce partial commit probability)
// 
// NOTE: Queries with aggregate results will not produce expected
// results right now.  For instance, running a count(*) on a table
// across all tablets will return one row per tablet.  In the future,
// the SQL parser and query engine can handle these more
// automatically. For now, clients will have to do the rollup at a
// higher level.

var (
	ErrNotConnected = fmt.Errorf("vt: not connected")
)

const (
	DefaultPortName = "_vtocc"
)

type VtClientError struct {
	msg     string
	partial bool
}

func (err VtClientError) Error() string {
	return err.msg
}

func (err VtClientError) Partial() bool {
	return err.partial
}

// Not thread safe, as per sql package.
type ShardedConn struct {
	zconn          zk.Conn
	zkKeyspacePath string
	dbType         string
	dbName         string
	stream         bool   // Use streaming RPC
	user           string // "" if not using auth
	password       string // "" iff userName is ""

	srvKeyspace *naming.SrvKeyspace
	// Keep a map per shard mapping tabletType to a real connection.
	// connByType []map[string]*Conn

	// Sorted list of the max keys for each shard.
	shardMaxKeys []string
	conns        []*tablet.VtConn

	timeout time.Duration // How long should we wait for a given operation?

	// Currently running transaction (or nil if not inside a transaction)
	currentTransaction *MetaTx
}

// FIXME(msolomon) Normally a connect method would actually connect up
// to the appropriate endpoints. In the distributed case, it's unclear
// that this is necessary.  You have to deal with transient failures
// anyway, so the whole system degenerates to managing connections on
// demand.
// zkKeyspaceSrvPath: /zk/local/vt/ns/<keyspace>
func Dial(zconn zk.Conn, zkKeyspaceSrvPath, dbType string, stream bool, timeout time.Duration, user, password string) (*ShardedConn, error) {
	sc := &ShardedConn{
		zconn:          zconn,
		zkKeyspacePath: zkKeyspaceSrvPath,
		dbType:         dbType,
		dbName:         "vt_" + path.Base(zkKeyspaceSrvPath),
		stream:         stream,
		user:           user,
		password:       password,
	}
	err := sc.readKeyspace()
	if err != nil {
		return nil, err
	}
	return sc, nil
}

func (sc *ShardedConn) Close() error {
	if sc.conns == nil {
		return nil
	}
	if sc.currentTransaction != nil {
		sc.rollback()
	}

	for _, conn := range sc.conns {
		if conn != nil {
			conn.Close()
		}
	}
	sc.conns = nil
	sc.srvKeyspace = nil
	sc.shardMaxKeys = nil
	return nil
}

func (sc *ShardedConn) readKeyspace() error {
	sc.Close()
	var err error
	sc.srvKeyspace, err = naming.ReadSrvKeyspace(sc.zconn, sc.zkKeyspacePath)
	if err != nil {
		return err
	}

	sc.conns = make([]*tablet.VtConn, len(sc.srvKeyspace.Shards))
	sc.shardMaxKeys = make([]string, len(sc.srvKeyspace.Shards))

	for i, srvShard := range sc.srvKeyspace.Shards {
		// FIXME(msolomon) do this as string, or make everyting in terms of KeyspaceId?
		sc.shardMaxKeys[i] = string(srvShard.KeyRange.End)
	}

	// Disabled for now.
	// sc.connByType = make([]map[string]*Conn, len(sc.srvKeyspace.Shards))
	// for i := 0; i < len(sc.connByType); i++ {
	// 	sc.connByType[i] = make(map[string]*Conn, 8)
	// }
	return nil
}

// A "transaction" that may be across and thus, not transactional at
// this point.
type MetaTx struct {
	// The connections involved in this transaction, in the order they
	// were added to the transaction.
	shardedConn *ShardedConn
	conns       []*tablet.VtConn
}

// makes sure the given transaction was issued a Begin() call
func (tx *MetaTx) begin(conn *tablet.VtConn) (err error) {
	for _, v := range tx.conns {
		if v == conn {
			return
		}
	}

	_, err = conn.Begin()
	if err != nil {
		// the caller will need to take care of the rollback,
		// and therefore issue a rollback on all pre-existing
		// transactions
		return err
	}
	tx.conns = append(tx.conns, conn)
	return nil
}

func (tx *MetaTx) Commit() (err error) {
	if tx.shardedConn.currentTransaction == nil {
		return tablet.ErrBadRollback
	}

	commit := true
	for _, conn := range tx.conns {
		if commit {
			if err = conn.Commit(); err != nil {
				commit = false
			}
		}
		if !commit {
			conn.Rollback()
		}
	}
	tx.shardedConn.currentTransaction = nil
	return err
}

func (tx *MetaTx) Rollback() error {
	if tx.shardedConn.currentTransaction == nil {
		return tablet.ErrBadRollback
	}
	var someErr error
	for _, conn := range tx.conns {
		if err := conn.Rollback(); err != nil {
			someErr = err
		}
	}
	tx.shardedConn.currentTransaction = nil
	return someErr
}

func (sc *ShardedConn) Begin() (driver.Tx, error) {
	if sc.srvKeyspace == nil {
		return nil, ErrNotConnected
	}
	if sc.currentTransaction != nil {
		return nil, tablet.ErrNoNestedTxn
	}
	tx := &MetaTx{sc, make([]*tablet.VtConn, 0, 32)}
	sc.currentTransaction = tx
	return tx, nil
}

func (sc *ShardedConn) rollback() error {
	if sc.currentTransaction == nil {
		return tablet.ErrBadRollback
	}
	var someErr error
	for _, conn := range sc.conns {
		if conn.TransactionId != 0 {
			if err := conn.Rollback(); err != nil {
				someErr = err
			}
		}
	}
	sc.currentTransaction = nil
	return someErr
}

// driver.Execer interface
func (sc *ShardedConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	bindVars := make(map[string]interface{})
	for i, v := range args {
		bindVars[fmt.Sprintf("v%d", i)] = v
	}
	return sc.ExecBind(query, bindVars)
}

func (sc *ShardedConn) ExecBind(query string, bindVars map[string]interface{}) (driver.Result, error) {
	if sc.srvKeyspace == nil {
		return nil, ErrNotConnected
	}
	shards, err := sqlparser.GetShardList(query, bindVars, sc.shardMaxKeys)
	if err != nil {
		return nil, err
	}
	if sc.stream {
		return sc.execBindOnShardsStream(query, bindVars, shards)
	}
	return sc.execBindOnShards(query, bindVars, shards)
}

func (sc *ShardedConn) QueryBind(query string, bindVars map[string]interface{}) (driver.Rows, error) {
	res, err := sc.ExecBind(query, bindVars)
	if err != nil {
		return nil, err
	}
	return res.(driver.Rows), nil
}

// FIXME(msolomon) define key interface "Keyer" or force a concrete type?
func (sc *ShardedConn) ExecBindWithKey(query string, bindVars map[string]interface{}, key interface{}) (driver.Result, error) {
	// FIXME(msolomon) this doesn't belong in the parser.
	shardIdx, err := sqlparser.FindShardForKey(key, sc.shardMaxKeys)
	if err != nil {
		return nil, err
	}
	if sc.stream {
		return sc.execBindOnShardsStream(query, bindVars, []int{shardIdx})
	}
	return sc.execBindOnShards(query, bindVars, []int{shardIdx})
}

type tabletResult struct {
	error
	*tablet.Result
}

func (sc *ShardedConn) execBindOnShards(query string, bindVars map[string]interface{}, shards []int) (metaResult *tablet.Result, err error) {
	rchan := make(chan tabletResult, len(shards))
	for _, shardIdx := range shards {
		go func(shardIdx int) {
			qr, err := sc.execBindOnShard(query, bindVars, shardIdx)
			if err != nil {
				rchan <- tabletResult{error: err}
			} else {
				rchan <- tabletResult{Result: qr.(*tablet.Result)}
			}
		}(shardIdx)
	}

	results := make([]tabletResult, len(shards))
	rowCount := int64(0)
	rowsAffected := int64(0)
	lastInsertId := int64(0)
	var hasError error
	for i := range results {
		results[i] = <-rchan
		if results[i].error != nil {
			hasError = results[i].error
			continue
		}
		affected, _ := results[i].RowsAffected()
		insertId, _ := results[i].LastInsertId()
		rowsAffected += affected
		if insertId > 0 {
			if lastInsertId == 0 {
				lastInsertId = insertId
			}
			// FIXME(msolomon) issue an error when you have multiple last inserts?
		}
		rowCount += results[i].RowsRetrieved()
	}

	// FIXME(msolomon) allow partial result set?
	if hasError != nil {
		return nil, fmt.Errorf("vt: partial result set (%v)", hasError)
	}

	for _, tr := range results {
		if tr.error != nil {
			return nil, tr.error
		}
		// FIXME(msolomon) This error message should be a const. Should this
		// be deferred until we get a next query?
		if tr.error != nil && tr.error.Error() == "retry: unavailable" {
			sc.readKeyspace()
		}
	}

	var fields []mproto.Field
	if len(results) > 0 {
		fields = results[0].Fields()
	}

	// check the schemas all match (both names and types)
	if len(results) > 1 {
		firstFields := results[0].Fields()
		for _, r := range results[1:] {
			fields := r.Fields()
			if len(fields) != len(firstFields) {
				return nil, fmt.Errorf("vt: column count mismatch: %v != %v", len(firstFields), len(fields))
			}
			for i, name := range fields {
				if name.Name != firstFields[i].Name {
					return nil, fmt.Errorf("vt: column[%v] name mismatch: %v != %v", i, name.Name, firstFields[i].Name)
				}
			}
		}
	}

	// Combine results.
	metaResult = tablet.NewResult(rowCount, rowsAffected, lastInsertId, fields)
	curIndex := 0
	rows := metaResult.Rows()
	for _, tr := range results {
		for _, row := range tr.Rows() {
			rows[curIndex] = row
			curIndex++
		}
	}

	return metaResult, nil
}

func (sc *ShardedConn) execBindOnShard(query string, bindVars map[string]interface{}, shardIdx int) (driver.Result, error) {
	if sc.conns[shardIdx] == nil {
		conn, err := sc.dial(shardIdx)
		if err != nil {
			return nil, err
		}
		sc.conns[shardIdx] = conn
	}
	conn := sc.conns[shardIdx]

	// if we haven't started the transaction on that shard and need to, now is the time
	if sc.currentTransaction != nil {
		err := sc.currentTransaction.begin(conn)
		if err != nil {
			return nil, err
		}
	}

	// Retries should have already taken place inside the tablet connection.
	// At this point, all that's left are more sinister failures.
	// FIXME(msolomon) reload just this shard unless the failure pertains to
	// needing to reload the entire keyspace.
	return conn.ExecBind(query, bindVars)
}

// when doing a streaming query, we send this structure back
type streamTabletResult struct {
	error
	row []driver.Value
}

// our streaming result, just aggregates from all streaming results
// it implements both driver.Result and driver.Rows
type multiStreamResult struct {
	cols []string

	// we keep track of how many shards are done
	count int
	done  int

	// results flow through this, maybe with errors
	rows chan streamTabletResult

	// set to the error we need to report to the last Next()
	err error
}

// driver.Result interface
func (*multiStreamResult) LastInsertId() (int64, error) {
	return 0, tablet.ErrNoLastInsertId
}

func (*multiStreamResult) RowsAffected() (int64, error) {
	return 0, tablet.ErrNoRowsAffected
}

// driver.Rows interface
func (sr *multiStreamResult) Columns() []string {
	return sr.cols
}

func (sr *multiStreamResult) Close() error {
	close(sr.rows)
	return nil
}

// read from the stream and gets the next value
// if one of the go routines returns an error, we want to save it and return it
// eventually. (except if it's EOF, then we just know that routine is done)
func (sr *multiStreamResult) Next(dest []driver.Value) error {
	if len(dest) != len(sr.cols) {
		return tablet.ErrFieldLengthMismatch
	}

	// we may need to read more than one value if we get a EOF
	var str streamTabletResult
	for {
		str = <-sr.rows
		if str.error == nil {
			break
		}

		// one of the streams finished or errored out
		sr.done += 1
		if str.error != io.EOF {
			// save this error
			sr.err = str.error
		}
		if sr.done == sr.count {
			if sr.err != nil {
				return sr.err
			} else {
				return io.EOF
			}
		}
	}
	for i, v := range str.row {
		dest[i] = v
	}
	return nil
}

func (sc *ShardedConn) execBindOnShardsStream(query string, bindVars map[string]interface{}, shards []int) (msr *multiStreamResult, err error) {
	// we synchronously do the execBind on each shard
	// so we can get the Columns from the first one
	// and check the others match them
	var cols []string
	qrs := make([]driver.Rows, len(shards))
	for i, shardIdx := range shards {
		qr, err := sc.execBindOnShard(query, bindVars, shardIdx)
		if err != nil {
			// FIXME(alainjobart) if the first queries went through
			// we need to cancel them
			return nil, err
		}

		// we know the result is a tablet.StreamResult,
		// and we use it as a driver.Rows
		qrs[i] = qr.(driver.Rows)

		// save the columns or check they match
		if i == 0 {
			cols = qrs[i].Columns()
		} else {
			ncols := qrs[i].Columns()
			if len(ncols) != len(cols) {
				return nil, fmt.Errorf("vt: column count mismatch: %v != %v", len(ncols), len(cols))
			}
			for i, name := range cols {
				if name != ncols[i] {
					return nil, fmt.Errorf("vt: column[%v] name mismatch: %v != %v", i, name, ncols[i])
				}
			}
		}
	}

	// now we create the result, its channel, and run background
	// routines to stream results
	msr = &multiStreamResult{cols: cols, count: len(shards), rows: make(chan streamTabletResult, 10*len(shards))}
	for i, shardIdx := range shards {
		go func(i, shardIdx int) {
			for {
				row := make([]driver.Value, len(cols))
				err := qrs[i].Next(row)
				if err != nil {
					// that will catch io.EOF as well
					msr.rows <- streamTabletResult{error: err}
					return
				}
				msr.rows <- streamTabletResult{row: row}
			}
		}(i, shardIdx)
	}

	return msr, nil
}

/*
type ClientQuery struct {
	Sql           string
	BindVariables map[string]interface{}
}

// FIXME(msolomon) There are multiple options for an efficient ExecMulti.
// * Use a special stmt object, buffer all statements, connections, etc and send when it's ready.
// * Take a list of (sql, bind) pairs and just send that - have to parse and route that anyway.
// * Probably need separate support for the a MultiTx too.
func (sc *ShardedConn) ExecuteBatch(queryList []ClientQuery, key interface{}) (*tabletserver.QueryResult, error) {
	shardIdx, err := sqlparser.FindShardForKey(key, sc.shardMaxKeys)
	shards := []int{shardIdx}

	if err = sc.tabletPrepare(shardIdx); err != nil {
		return nil, err
	}

	reqs := make([]tabletserver.Query, len(queryList))
	for i, cq := range queryList {
		reqs[i] = tabletserver.Query{
			Sql:           cq.Sql,
			BindVariables: cq.BindVariables,
			TransactionId: sc.conns[shardIdx].TransactionId,
			SessionId:     sc.conns[shardIdx].SessionId,
		}
	}
	res := new(tabletserver.QueryResult)
	err = sc.conns[shardIdx].Call("SqlQuery.ExecuteBatch", reqs, res)
	if err != nil {
		return nil, err
	}
	return res, nil
}
*/

func (sc *ShardedConn) dial(shardIdx int) (conn *tablet.VtConn, err error) {
	// FIXME(msolomon) do we need to send the key range we expect, or
	// any additional validation?
	addrs := sc.srvKeyspace.Shards[shardIdx].AddrsByType[sc.dbType]
	srvs, err := naming.SrvEntries(&addrs, DefaultPortName)
	if err != nil {
		return nil, err
	}

	// Try to connect to any address.
	for _, srv := range srvs {
		name := naming.SrvAddr(srv) + "/" + sc.dbName
		if sc.user != "" {
			name = sc.user + ":" + sc.password + "@" + name
		}
		conn, err = tablet.DialVtdb(name, sc.stream, tablet.DefaultTimeout)
		if err == nil {
			return conn, nil
		}
	}
	return nil, err
}

type MetaStmt struct {
	shardedConn *ShardedConn
	conns       []*tablet.VtConn
	query       string
}

// driver.Stmt interface
func (stmt *MetaStmt) Close() error {
	stmt.shardedConn = nil
	stmt.conns = nil
	stmt.query = ""
	return nil
}

func (*MetaStmt) NumInput() int {
	return -1
}

func (stmt *MetaStmt) Exec(args []driver.Value) (driver.Result, error) {
	// let the connection handle this
	return stmt.shardedConn.Exec(stmt.query, args)
}

func (stmt *MetaStmt) Query(args []driver.Value) (driver.Rows, error) {
	// FIXME(msolomon) how to handle multiple exec? MultiResult?
	// we use driver.Execer interface, we know it's a Result return,
	// and our Result implements driver.Rows
	// (or a StreamResult that does too)
	res, err := stmt.shardedConn.Exec(stmt.query, args)
	if err != nil {
		return nil, err
	}
	return res.(driver.Rows), nil
}

func (sc *ShardedConn) Prepare(query string) (driver.Stmt, error) {
	conns := make([]*tablet.VtConn, 0, 16)
	stmt := &MetaStmt{sc, conns, query}
	return stmt, nil
}

type sDriver struct {
	zconn  zk.Conn
	stream bool
}

// for direct zk connection: vtzk://host:port/zkpath/dbType
// we always use a MetaConn, host and port are ignored.
// the driver name dictates if we use zk or zkocc, and streaming or not
//
// if user and password are specified in the URL, they will be used
// for each DB connection to the tablet's vttablet processes
func (driver *sDriver) Open(name string) (sc driver.Conn, err error) {
	if !strings.HasPrefix(name, "vtzk://") {
		// add a default protocol talking to zk
		name = "vtzk://" + name
	}
	u, err := url.Parse(name)
	if err != nil {
		return nil, err
	}

	dbi, dbType := path.Split(u.Path)
	dbi = strings.TrimRight(dbi, "/")
	var user, password string
	if u.User != nil {
		user = u.User.Username()
		var ok bool
		password, ok = u.User.Password()
		if !ok {
			return nil, fmt.Errorf("vt: need a password if a user is specified")
		}
	}
	return Dial(driver.zconn, dbi, dbType, driver.stream, tablet.DefaultTimeout, user, password)
}

func init() {
	zconn := zk.NewMetaConn(5*time.Second, false)
	zkoccconn := zk.NewMetaConn(5*time.Second, true)
	sql.Register("vtdb", &sDriver{zconn, false})
	sql.Register("vtdb-zkocc", &sDriver{zkoccconn, false})
	sql.Register("vtdb-streaming", &sDriver{zconn, true})
	sql.Register("vtdb-zkocc-streaming", &sDriver{zkoccconn, true})
	sql.Register("vtdb-streaming-zkocc", &sDriver{zkoccconn, true})
}
