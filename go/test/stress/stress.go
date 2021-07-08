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

	Stresser struct {
		cfg      *Config
		doneCh   chan Result
		tbls     []*table
		duration time.Duration
		start    time.Time
		t        *testing.T
		finish   bool
	}

	Config struct {
		StopAfter       time.Duration
		PrintErrLogs    bool
		NumberOfTables  int
		TableNamePrefix string
		InsertInterval  time.Duration
		DeleteInterval  time.Duration
		SelectInterval  time.Duration
		SelectLimit     int
		ConnParams      *mysql.ConnParams
		MaxClient       int
	}
)

var DefaultConfig = &Config{
	StopAfter:       30 * time.Second,
	PrintErrLogs:    false,
	NumberOfTables:  100,
	TableNamePrefix: "stress_t",
	InsertInterval:  10 * time.Microsecond,
	DeleteInterval:  15 * time.Microsecond,
	SelectInterval:  2 * time.Microsecond,
	SelectLimit:     500,
	MaxClient:       10,
}

func New(t *testing.T, cfg *Config) *Stresser {
	return &Stresser{
		cfg:    cfg,
		doneCh: make(chan Result),
		t:      t,
	}
}

func generateNewTables(nb int) []*table {
	tbls := make([]*table, 0, nb)
	for i := 0; i < nb; i++ {
		tbls = append(tbls, &table{
			name: fmt.Sprintf("stress_t%d", i),
		})
	}
	return tbls
}

func (s *Stresser) createTables(nb int) []*table {
	conn := newClient(s.t, s.cfg.ConnParams)
	defer conn.Close()

	tbls := generateNewTables(nb)
	for _, tbl := range tbls {
		s.exec(conn, fmt.Sprintf(templateNewTable, tbl.name))
	}
	return tbls
}

func (s *Stresser) SetConn(conn *mysql.ConnParams) *Stresser {
	s.cfg.ConnParams = conn
	return s
}

func (s *Stresser) Start() *Stresser {
	fmt.Println("Starting load testing ...")
	s.tbls = s.createTables(100)
	go s.startClients()
	return s
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

	timeout := time.After(s.cfg.StopAfter)

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

	query := fmt.Sprintf("select * from %s limit 500", s.tbls[tblI].name)
	expLength := s.tbls[tblI].rows
	if expLength > 500 {
		expLength = 500
	}
	if s.assertLength(conn, query, expLength) {
		r.selects.success++
	} else {
		r.selects.failure++
	}
}

func (s *Stresser) Wait(timeout time.Duration) {
	timeoutCh := time.After(timeout)
	select {
	case res := <-s.doneCh:
		res.Print(s.duration.Seconds())
	case <-timeoutCh:
		s.finish = true
		res := <-s.doneCh
		res.Print(s.duration.Seconds())
	}
}
