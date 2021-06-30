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
	result struct {
		countSelect int
		countInsert int
	}

	table struct {
		name         string
		rows, nextID int
		mu           sync.Mutex
	}

	stresser struct {
		tbls       []*table
		connParams mysql.ConnParams
		maxClient  int
		duration   time.Duration
	}
)

func (r result) printQPS(seconds float64) {
	fmt.Printf(`QPS:
	select: %d
	insert: %d
	---------
	total:	%d
`, r.countSelect/int(seconds), r.countInsert/int(seconds), r.countInsert+r.countSelect/int(seconds))
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

func Start(t *testing.T, params mysql.ConnParams, duration time.Duration, done chan result) {
	fmt.Println("Starting load testing ...")

	s := stresser{
		tbls:       createTables(t, params, 100),
		connParams: params,
		maxClient:  10,
		duration:   duration,
	}

	resultCh := make(chan result, s.maxClient)

	for i := 0; i < s.maxClient; i++ {
		go s.startStressClient(t, resultCh)
	}

	perClientResults := make([]result, 0, s.maxClient)
	for i := 0; i < s.maxClient; i++ {
		newResult := <-resultCh
		perClientResults = append(perClientResults, newResult)
	}

	var finalResult result
	for _, r := range perClientResults {
		finalResult.countSelect += r.countSelect
		finalResult.countInsert += r.countInsert
	}
	done <- finalResult
}

func (s *stresser) startStressClient(t *testing.T, resultCh chan result) {
	conn := newClient(t, s.connParams)
	defer conn.Close()

	var res result

	timeout := time.After(s.duration)
	for {
		select {
		case <-timeout:
			resultCh <- res
			return
		case <-time.After(15 * time.Microsecond):
			s.insertToRandomTable(t, conn)
			res.countInsert++
		case <-time.After(1 * time.Microsecond):
			s.selectFromRandomTable(t, conn)
			res.countSelect++
		}
	}
}

func (s *stresser) insertToRandomTable(t *testing.T, conn *mysql.Conn) {
	tblI := rand.Int() % len(s.tbls)
	s.tbls[tblI].mu.Lock()
	defer s.tbls[tblI].mu.Unlock()

	query := fmt.Sprintf("insert into %s(id, val) values(%d, 'name')", s.tbls[tblI].name, s.tbls[tblI].nextID)
	s.tbls[tblI].nextID++
	s.tbls[tblI].rows++
	exec(t, conn, query)
}

func (s *stresser) selectFromRandomTable(t *testing.T, conn *mysql.Conn) {
	tblI := rand.Int() % len(s.tbls)
	s.tbls[tblI].mu.Lock()
	defer s.tbls[tblI].mu.Unlock()

	query := fmt.Sprintf("select * from %s limit 500", s.tbls[tblI].name)
	expLength := s.tbls[tblI].rows
	if expLength > 500 {
		expLength = 500
	}
	assertLength(t, conn, query, expLength)
}
