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
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
)

const (
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

	// Stresser is responsible for stressing a Vitess cluster.
	// It can be configured through the use of Config.
	Stresser struct {
		cfg      *Config
		doneCh   chan Result
		tbls     []*table
		duration time.Duration
		start    time.Time
		t        *testing.T
		finish   bool
	}

	// Config contains all of the Stresser configuration.
	Config struct {
		// MaximumDuration for which each client can stress the cluster.
		MaximumDuration time.Duration

		// PrintErrLogs enables or disables the rendering of MySQL error logs.
		PrintErrLogs bool

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
	}
)

// DefaultConfig is the default configuration used by the stresser.
var DefaultConfig = &Config{
	MaximumDuration: 120 * time.Second,
	PrintErrLogs:    false,
	NumberOfTables:  100,
	TableNamePrefix: "stress_t",
	InsertInterval:  10 * time.Microsecond,
	DeleteInterval:  15 * time.Microsecond,
	SelectInterval:  2 * time.Microsecond,
	SelectLimit:     500,
	MaxClient:       10,
}

// New creates a new Stresser based on the given Config.
func New(t *testing.T, cfg *Config) *Stresser {
	return &Stresser{
		cfg:    cfg,
		doneCh: make(chan Result),
		t:      t,
	}
}

// Stop the stresser immediately and print the results.
func (s *Stresser) Stop() {
	s.StopAfter(0)
}

// StopAfter stops the stresser after a given duration and print the results.
func (s *Stresser) StopAfter(after time.Duration) {
	timeoutCh := time.After(after)
	select {
	case res := <-s.doneCh:
		res.Print(s.duration.Seconds())
	case <-timeoutCh:
		s.finish = true
		res := <-s.doneCh
		res.Print(s.duration.Seconds())
	}
}

// SetConn allows us to change the mysql.ConnParams of our stresser at runtime.
// Setting a new mysql.ConnParams will automatically create new mysql client using
// the new configuration.
func (s *Stresser) SetConn(conn *mysql.ConnParams) *Stresser {
	s.cfg.ConnParams = conn
	return s
}

// Start the stresser.
func (s *Stresser) Start() *Stresser {
	fmt.Println("Starting load testing ...")
	s.tbls = s.createTables(100)
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

func (s *Stresser) startClients() {
	s.start = time.Now()

	resultCh := make(chan Result, s.cfg.MaxClient)
	for i := 0; i < s.cfg.MaxClient; i++ {
		go s.startStressClient(resultCh)
	}

	perClientResults := make([]Result, 0, s.cfg.MaxClient)
	for i := 0; i < s.cfg.MaxClient; i++ {
		newResult := <-resultCh
		perClientResults = append(perClientResults, newResult)
	}
	s.duration = time.Since(s.start)

	var finalResult Result
	for _, r := range perClientResults {
		finalResult.inserts = sumQueryCounts(finalResult.inserts, r.inserts)
		finalResult.selects = sumQueryCounts(finalResult.selects, r.selects)
		finalResult.deletes = sumQueryCounts(finalResult.deletes, r.deletes)
	}
	s.doneCh <- finalResult
}

func (s *Stresser) startStressClient(resultCh chan Result) {
	connParams := s.cfg.ConnParams
	conn := newClient(s.t, connParams)
	defer conn.Close()

	var res Result

	timeout := time.After(s.cfg.MaximumDuration - time.Since(s.start))

outer:
	for !s.finish {
		if connParams != s.cfg.ConnParams {
			connParams = s.cfg.ConnParams
			conn.Close()
			conn = newClient(s.t, connParams)
		}
		select {
		case <-timeout:
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

func (s *Stresser) deleteFromRandomTable(conn *mysql.Conn, r *Result) {
	tblI := rand.Int() % len(s.tbls)
	s.tbls[tblI].mu.Lock()
	defer s.tbls[tblI].mu.Unlock()

	query := fmt.Sprintf("delete from %s where id = %d", s.tbls[tblI].name, s.tbls[tblI].nextID-1)
	if s.exec(conn, query) != nil {
		s.tbls[tblI].nextID--
		s.tbls[tblI].rows--
		r.deletes.success++
	} else {
		r.deletes.failure++
	}
}

func (s *Stresser) insertToRandomTable(conn *mysql.Conn, r *Result) {
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
	}
}

func (s *Stresser) selectFromRandomTable(conn *mysql.Conn, r *Result) {
	tblI := rand.Int() % len(s.tbls)
	s.tbls[tblI].mu.Lock()
	defer s.tbls[tblI].mu.Unlock()

	query := fmt.Sprintf("select * from %s limit %d", s.tbls[tblI].name, s.cfg.SelectLimit)
	expLength := s.tbls[tblI].rows
	if expLength > s.cfg.SelectLimit {
		expLength = s.cfg.SelectLimit
	}
	if s.assertLength(conn, query, expLength) {
		r.selects.success++
	} else {
		r.selects.failure++
	}
}
