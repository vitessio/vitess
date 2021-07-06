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
	Result struct {
		countSelect int
		countInsert int
	}

	table struct {
		name         string
		rows, nextID int
		mu           sync.Mutex
	}

	Stresser struct {
		doneCh     chan Result
		tbls       []*table
		connParams mysql.ConnParams
		maxClient  int
		duration   time.Duration
		t          *testing.T
	}
)

func (r Result) PrintQPS(seconds float64) {
	fmt.Printf(`QPS:
	select: %d
	insert: %d
	---------
	total:	%d
`, r.countSelect/int(seconds), r.countInsert/int(seconds), (r.countInsert+r.countSelect)/int(seconds))
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

func createTables(t *testing.T, params mysql.ConnParams, nb int) []*table {
	conn := newClient(t, params)
	defer conn.Close()

	tbls := generateNewTables(nb)
	for _, tbl := range tbls {
		exec(t, conn, fmt.Sprintf(templateNewTable, tbl.name))
	}
	return tbls
}

func New(t *testing.T, conn mysql.ConnParams, duration time.Duration) *Stresser {
	return &Stresser{
		doneCh:     make(chan Result),
		t:          t,
		connParams: conn,
		duration:   duration,
		maxClient:  10,
	}
}

func (s *Stresser) Start() {
	fmt.Println("Starting load testing ...")
	s.tbls = createTables(s.t, s.connParams, 100)
	go s.startClients()
}

func (s *Stresser) startClients() {
	resultCh := make(chan Result, s.maxClient)
	for i := 0; i < s.maxClient; i++ {
		go s.startStressClient(resultCh)
	}

	perClientResults := make([]Result, 0, s.maxClient)
	for i := 0; i < s.maxClient; i++ {
		newResult := <-resultCh
		perClientResults = append(perClientResults, newResult)
	}

	var finalResult Result
	for _, r := range perClientResults {
		finalResult.countSelect += r.countSelect
		finalResult.countInsert += r.countInsert
	}
	s.doneCh <- finalResult
}

func (s *Stresser) startStressClient(resultCh chan Result) {
	conn := newClient(s.t, s.connParams)
	defer conn.Close()

	var res Result

	timeout := time.After(s.duration)
	for {
		select {
		case <-timeout:
			resultCh <- res
			return
		case <-time.After(15 * time.Microsecond):
			s.insertToRandomTable(conn)
			res.countInsert++
		case <-time.After(1 * time.Microsecond):
			s.selectFromRandomTable(conn)
			res.countSelect++
		}
	}
}

func (s *Stresser) insertToRandomTable(conn *mysql.Conn) {
	tblI := rand.Int() % len(s.tbls)
	s.tbls[tblI].mu.Lock()
	defer s.tbls[tblI].mu.Unlock()

	query := fmt.Sprintf("insert into %s(id, val) values(%d, 'name')", s.tbls[tblI].name, s.tbls[tblI].nextID)
	s.tbls[tblI].nextID++
	s.tbls[tblI].rows++
	exec(s.t, conn, query)
}

func (s *Stresser) selectFromRandomTable(conn *mysql.Conn) {
	tblI := rand.Int() % len(s.tbls)
	s.tbls[tblI].mu.Lock()
	defer s.tbls[tblI].mu.Unlock()

	query := fmt.Sprintf("select * from %s limit 500", s.tbls[tblI].name)
	expLength := s.tbls[tblI].rows
	if expLength > 500 {
		expLength = 500
	}
	assertLength(s.t, conn, query, expLength)
}

func (s *Stresser) Wait(timeout time.Duration) {
	timeoutCh := time.After(timeout)
	select {
	case res := <-s.doneCh:
		res.PrintQPS(s.duration.Seconds())
	case <-timeoutCh:
		s.t.Fatalf("Test timed out")
	}
}
