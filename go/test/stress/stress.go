/*
Copyright 2021 The Vitess Authors.

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

package stress

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
)

const (
	// Template used to create new table in the database.
	// TODO: have dynamic schemas
	templateNewTable = `create table %s (
	id bigint,
	val varchar(64),
	primary key (id)
) Engine=InnoDB
`
)

type (
	table struct {
		name         string
		rows, nextID int
		mu           sync.Mutex
	}

	// Stresser is responsible for stressing a Vitess cluster based on a given Config.
	// Stressing a Vitess cluster is achieved by spawning several clients that continuously
	// send queries to the cluster.
	//
	// The Stresser uses SELECT, INSERT and DELETE statements to stress the cluster. Queries
	// are made against tables that are generated when calling Stresser.Start().
	// For each query, we keep its status (failed or succeeded) and at the end of the stress,
	// when calling Stresser.Stop() or Stresser.StopAfter(), we assert that all queries have
	// succeeded, otherwise the Stresser will fail the test.
	//
	// This behavior can be changed by the use of Stresser.AllowFailure(bool) and the AllowFailure
	// field of Config.
	//
	// Below is an a sample usage of the Stresser:
	//
	//		// copy the DefaultConfig and set your own mysql.ConnParams
	//		cfg := stress.DefaultConfig
	//		cfg.ConnParams = &mysql.ConnParams{Port: 8888, Host: "localhost", DbName: "ks"}
	//		s := stress.New(t, cfg).Start()
	//
	//		// your end to end test here
	//
	//		s.Stop() // stop the Stresser and assert its results
	//
	Stresser struct {
		cfg      Config
		doneCh   chan result
		tbls     []*table
		duration time.Duration
		start    time.Time
		t        *testing.T
		finish   uint32
		cfgMu    sync.Mutex
	}

	// Config contains all of the Stresser configuration.
	Config struct {
		// MaximumDuration during which each client can stress the cluster.
		MaximumDuration time.Duration

		// MinimumDuration during which each client must stress the cluster.
		MinimumDuration time.Duration

		// PrintErrLogs enables or disables the rendering of MySQL error logs.
		PrintErrLogs bool

		// PrintLogs enables or disables the rendering of Stresser logs.
		PrintLogs bool

		// NumberOfTables to create in the cluster.
		NumberOfTables int

		// TableNamePrefix defines which prefix will be used for name of the auto-generated tables.
		TableNamePrefix string

		// InsertInterval defines at which interval each insert queries should be sent.
		InsertInterval time.Duration

		// DeleteInterval defines at which interval each delete queries should be sent.
		DeleteInterval time.Duration

		// SelectInterval defines at which interval each select queries should be sent.
		SelectInterval time.Duration

		// SelectLimit defines the maximum number of row select queries can query at once.
		SelectLimit int

		// ConnParams is the mysql.ConnParams that should be use to create new clients.
		ConnParams *mysql.ConnParams

		// MaxClient is the maximum number of concurrent client stressing the cluster.
		MaxClient int

		// AllowFailure determines whether failing queries are allowed or not.
		// All queries that fail while this setting is set to true will not be counted
		// by Stresser.Stop's assertion.
		AllowFailure bool
	}
)

// DefaultConfig is the default configuration used by the stresser.
var DefaultConfig = Config{
	MaximumDuration: 120 * time.Second,
	MinimumDuration: 1 * time.Second,
	PrintErrLogs:    false,
	PrintLogs:       false,
	NumberOfTables:  100,
	TableNamePrefix: "stress_t",
	InsertInterval:  10 * time.Microsecond,
	DeleteInterval:  15 * time.Microsecond,
	SelectInterval:  2 * time.Microsecond,
	SelectLimit:     500,
	MaxClient:       10,
	AllowFailure:    false,
}

// AllowFailure will set the AllowFailure setting to the given value.
// Allowing failure means that all incoming queries that fail will be
// counted in result's QPS and total queries, however they will not
// be marked as "meaningful failure". Meaningful failures represent the
// failures that must fail the current test.
func (s *Stresser) AllowFailure(allow bool) {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.AllowFailure = allow
}

// New creates a new Stresser based on the given Config.
func New(t *testing.T, cfg Config) *Stresser {
	return &Stresser{
		cfg:    cfg,
		doneCh: make(chan result),
		t:      t,
	}
}

// Stop the Stresser immediately once Config.MinimumDuration is reached.
// To override Config.MinimumDuration, one can call Stresser.StopAfter with
// a value of 0.
// Once the Stresser has stopped, the function asserts that all results are
// successful, and then prints them to the standard output.
func (s *Stresser) Stop() {
	if time.Since(s.start) > s.cfg.MinimumDuration {
		s.StopAfter(0)
	} else {
		s.StopAfter(s.cfg.MinimumDuration - time.Since(s.start))
	}
}

// StopAfter stops the Stresser after the given duration. The function will then
// assert that all the results are successful, and finally prints them to the standard
// output.
func (s *Stresser) StopAfter(after time.Duration) {
	if s.start.IsZero() {
		s.t.Log("Load testing was not started.")
		return
	}
	timeoutCh := time.After(after)
	select {
	case res := <-s.doneCh:
		if s.cfg.PrintLogs {
			res.print(s.t.Logf, s.duration.Seconds())
		}
		if !res.assert() {
			s.t.Errorf("Requires no failed queries")
		}
	case <-timeoutCh:
		atomic.StoreUint32(&s.finish, 1)
		res := <-s.doneCh
		if s.cfg.PrintLogs {
			res.print(s.t.Logf, s.duration.Seconds())
		}
		if !res.assert() {
			s.t.Errorf("Requires no failed queries")
		}
	}
}

// SetConn allows us to change the mysql.ConnParams of a Stresser at runtime.
// Setting a new mysql.ConnParams will automatically create new MySQL client using
// the new configuration.
func (s *Stresser) SetConn(conn *mysql.ConnParams) *Stresser {
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()
	s.cfg.ConnParams = conn
	return s
}

// Start stressing the Vitess cluster.
// This method will start by creating the MySQL tables in the Vitess cluster based
// on the maximum number of table set through Config.NumberOfTables.
// The method will then start a goroutine that will spawn one or more clients.
// These clients will be responsible for stressing the cluster until Config.MaximumDuration
// is reached, or until Stresser.Stop() or Stresser.StopAfter() are called.
//
// This method returns a pointer to its Stresser to allow chained function call, like:
//
//	s := stress.New(t, cfg).Start()
//	s.Stop()
func (s *Stresser) Start() *Stresser {
	if s.cfg.PrintLogs {
		s.t.Log("Starting load testing ...")
	}
	s.tbls = s.createTables(s.cfg.NumberOfTables)
	s.start = time.Now()
	go s.startClients()
	return s
}

func generateNewTables(prefix string, nb int) []*table {
	tbls := make([]*table, 0, nb)
	for i := 0; i < nb; i++ {
		tbls = append(tbls, &table{
			name: fmt.Sprintf("%s%d", prefix, i),
		})
	}
	return tbls
}

func (s *Stresser) createTables(nb int) []*table {
	conn := newClient(s.t, s.cfg.ConnParams)
	defer conn.Close()

	tbls := generateNewTables(s.cfg.TableNamePrefix, nb)
	for _, tbl := range tbls {
		s.exec(conn, fmt.Sprintf(templateNewTable, tbl.name))
	}
	return tbls
}

// startClients is responsible for concurrently starting all the clients,
// fetching their results, and computing a single final result which is
// then publish in Stresser.doneCh.
func (s *Stresser) startClients() {
	maxClient := s.cfg.MaxClient
	resultCh := make(chan result, maxClient)

	// Start the concurrent clients.
	for i := 0; i < maxClient; i++ {
		go s.startStressClient(resultCh)
	}

	// Wait for the different clients to publish their results.
	perClientResults := make([]result, 0, maxClient)
	for i := 0; i < maxClient; i++ {
		newResult := <-resultCh
		perClientResults = append(perClientResults, newResult)
	}

	// Calculate how long it took for all the client to finish stressing
	// the cluster.
	s.duration = time.Since(s.start)

	// Based on all the clients' results, compute a single result.
	var finalResult result
	for _, r := range perClientResults {
		finalResult.inserts = sumQueryCounts(finalResult.inserts, r.inserts)
		finalResult.selects = sumQueryCounts(finalResult.selects, r.selects)
		finalResult.deletes = sumQueryCounts(finalResult.deletes, r.deletes)
	}
	s.doneCh <- finalResult
}

// startStressClient creates a client that will stress the cluster.
// This function is supposed to be called as many times as we want
// to have concurrent clients stressing the cluster.
// Once the client is done stressing the cluster, results are published
// in the given chan result.
func (s *Stresser) startStressClient(resultCh chan result) {
	s.cfgMu.Lock()
	connParams := s.cfg.ConnParams
	s.cfgMu.Unlock()

	conn := newClient(s.t, connParams)
	defer conn.Close()

	var res result

	// Create a timeout based on the Stresser maximum duration and the time
	// that has already elapsed since the Stresser was started.
	timeout := time.After(s.cfg.MaximumDuration - time.Since(s.start))

outer:
	for !s.finished() {

		// Update the connection parameters is Stresser has new ones, and
		// create a new client using the new parameters.
		// This allows us to change the target (server we are stressing) at
		// runtime without having to create a new Stresser.
		s.cfgMu.Lock()
		if connParams != s.cfg.ConnParams {
			connParams = s.cfg.ConnParams
			conn.Close()
			conn = newClient(s.t, connParams)
		}
		s.cfgMu.Unlock()

		select {
		case <-timeout: // Case where the Stresser has reached its maximum duration
			break outer
		case <-time.After(s.cfg.DeleteInterval):
			s.deleteFromRandomTable(conn, &res)
		case <-time.After(s.cfg.InsertInterval):
			s.insertToRandomTable(conn, &res)
		case <-time.After(s.cfg.SelectInterval):
			s.selectFromRandomTable(conn, &res)
		}
	}
	resultCh <- res
}

func (s *Stresser) finished() bool {
	return atomic.LoadUint32(&s.finish) == 1
}

// deleteFromRandomTable will delete the last row of a random table.
// If the random table contains no row, the query will not be sent.
func (s *Stresser) deleteFromRandomTable(conn *mysql.Conn, r *result) {
	tblI := rand.Int() % len(s.tbls)
	s.tbls[tblI].mu.Lock()
	defer s.tbls[tblI].mu.Unlock()

	// no row to delete
	if s.tbls[tblI].rows == 0 {
		return
	}

	query := fmt.Sprintf("delete from %s where id = %d", s.tbls[tblI].name, s.tbls[tblI].nextID-1)
	if s.exec(conn, query) != nil {
		s.tbls[tblI].nextID--
		s.tbls[tblI].rows--
		r.deletes.success++
	} else {
		r.deletes.failure++
		s.cfgMu.Lock()
		if !s.cfg.AllowFailure {
			r.deletes.meaningfulFailure++
		}
		s.cfgMu.Unlock()
	}
}

// insertToRandomTable inserts a new row into a random table.
func (s *Stresser) insertToRandomTable(conn *mysql.Conn, r *result) {
	tblI := rand.Int() % len(s.tbls)
	s.tbls[tblI].mu.Lock()
	defer s.tbls[tblI].mu.Unlock()

	query := fmt.Sprintf("insert into %s(id, val) values(%d, 'name')", s.tbls[tblI].name, s.tbls[tblI].nextID)
	if s.exec(conn, query) != nil {
		s.tbls[tblI].nextID++
		s.tbls[tblI].rows++
		r.inserts.success++
	} else {
		r.inserts.failure++
		s.cfgMu.Lock()
		if !s.cfg.AllowFailure {
			r.inserts.meaningfulFailure++
		}
		s.cfgMu.Unlock()
	}
}

// selectFromRandomTable selects all the rows (up to Config.SelectLimit) of a
// random table. If the table contains no row, the query will not be sent.
func (s *Stresser) selectFromRandomTable(conn *mysql.Conn, r *result) {
	tblI := rand.Int() % len(s.tbls)
	s.tbls[tblI].mu.Lock()
	defer s.tbls[tblI].mu.Unlock()

	// no row to select
	if s.tbls[tblI].rows == 0 {
		return
	}

	query := fmt.Sprintf("select * from %s limit %d", s.tbls[tblI].name, s.cfg.SelectLimit)
	expLength := s.tbls[tblI].rows
	if expLength > s.cfg.SelectLimit {
		expLength = s.cfg.SelectLimit
	}
	if s.assertLength(conn, query, expLength) {
		r.selects.success++
	} else {
		r.selects.failure++
		s.cfgMu.Lock()
		if !s.cfg.AllowFailure {
			r.selects.meaningfulFailure++
		}
		s.cfgMu.Unlock()
	}
}
