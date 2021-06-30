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
	"testing"
	"time"

	"vitess.io/vitess/go/mysql"
)

type result struct {
	countSelect int
}

func Start(t *testing.T, params mysql.ConnParams) {
	insertInitialTable(t, params)

	fmt.Println("Starting load testing ...")

	clientLimit := 5
	duration := 2 * time.Second

	resultCh := make(chan result, clientLimit)

	for i := 0; i < clientLimit; i++ {
		go startStressClient(t, duration, resultCh, params)
	}

	perClientResults := make([]result, 0, clientLimit)
	for i := 0; i < clientLimit; i++ {
		newResult := <-resultCh
		perClientResults = append(perClientResults, newResult)
	}

	var finalResult result
	for _, r := range perClientResults {
		finalResult.countSelect += r.countSelect
	}
	finalResult.printQPS(duration.Seconds())
}

func (r result) printQPS(seconds float64) {
	fmt.Printf(`QPS:
select: %d
`, r.countSelect/int(seconds))
}

func insertInitialTable(t *testing.T, params mysql.ConnParams) {
	conn := newClient(t, params)
	defer conn.Close()

	// TODO: move to `insert` case
	exec(t, conn, `insert into main(id, val) values(0,'test'),(1,'value')`)
}

func startStressClient(t *testing.T, duration time.Duration, resultCh chan result, params mysql.ConnParams) {
	conn := newClient(t, params)
	defer conn.Close()

	var res result

	timeout := time.After(duration)
	for {
		select {
		case <-timeout:
			resultCh <- res
			return
		case <-time.After(1 * time.Microsecond): // selects
			assertMatches(t, conn, `select id from main`, `[[INT64(0)] [INT64(1)]]`)
			res.countSelect++
		}
	}
}
