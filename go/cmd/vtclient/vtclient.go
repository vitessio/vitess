/*
Copyright 2017 Google Inc.

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

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/olekukonko/tablewriter"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vitessdriver"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	usage = `
vtclient connects to a vtgate server using the standard go driver API.
Version 3 of the API is used, we do not send any hint to the server.

For query bound variables, we assume place-holders in the query string
in the form of :v1, :v2, etc.

Examples:

  $ vtclient -server vtgate:15991 "SELECT * FROM messages"

  $ vtclient -server vtgate:15991 -target '@master' -bind_variables '[ 12345, 1, "msg 12345" ]' "INSERT INTO messages (page,time_created_ns,message) VALUES (:v1, :v2, :v3)"

`
	server        = flag.String("server", "", "vtgate server to connect to")
	timeout       = flag.Duration("timeout", 30*time.Second, "timeout for queries")
	streaming     = flag.Bool("streaming", false, "use a streaming query")
	bindVariables = newBindvars("bind_variables", "bind variables as a json list")
	targetString  = flag.String("target", "", "keyspace:shard@tablet_type")
	jsonOutput    = flag.Bool("json", false, "Output JSON instead of human-readable table")
	parallel      = flag.Int("parallel", 1, "DMLs only: Number of threads executing the same query in parallel. Useful for simple load testing.")
	count         = flag.Int("count", 1, "DMLs only: Number of times each thread executes the query. Useful for simple, sustained load testing.")
	minSeqID      = flag.Int("min_sequence_id", 0, "min sequence ID to generate. When max_sequence_id > min_sequence_id, for each query, a number is generated in [min_sequence_id, max_sequence_id) and attached to the end of the bind variables.")
	maxSeqID      = flag.Int("max_sequence_id", 0, "max sequence ID.")
	useRandom     = flag.Bool("use_random_sequence", false, "use random sequence for generating [min_sequence_id, max_sequence_id)")
	qps           = flag.Int("qps", 0, "queries per second to throttle each thread at.")
)

var (
	seqChan = make(chan int, 10)
)

func init() {
	flag.Usage = func() {
		_, _ = fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		_, _ = fmt.Fprint(os.Stderr, usage)
	}
}

type bindvars []interface{}

func (bv *bindvars) String() string {
	b, err := json.Marshal(bv)
	if err != nil {
		return err.Error()
	}
	return string(b)
}

func (bv *bindvars) Set(s string) (err error) {
	err = json.Unmarshal([]byte(s), &bv)
	if err != nil {
		return err
	}
	// json reads all numbers as float64
	// So, we just ditch floats for bindvars
	for i, v := range *bv {
		if f, ok := v.(float64); ok {
			if f > 0 {
				(*bv)[i] = uint64(f)
			} else {
				(*bv)[i] = int64(f)
			}
		}
	}

	return nil
}

// For internal flag compatibility
func (bv *bindvars) Get() interface{} {
	return bv
}

func newBindvars(name, usage string) *bindvars {
	var bv bindvars
	flag.Var(&bv, name, usage)
	return &bv
}

func main() {
	defer logutil.Flush()

	qr, err := run()
	if err != nil {
		log.Exit(err)
	}
	if *jsonOutput && qr != nil {
		data, err := json.MarshalIndent(qr, "", "  ")
		if err != nil {
			log.Exitf("cannot marshal data: %v", err)
		}
		fmt.Print(string(data))
		return
	}

	qr.print()
}

func run() (*results, error) {
	flag.Parse()
	args := flag.Args()

	if len(args) == 0 {
		flag.Usage()
		return nil, errors.New("no arguments provided. See usage above")
	}
	if len(args) > 1 {
		return nil, errors.New("no additional arguments after the query allowed")
	}

	if *maxSeqID > *minSeqID {
		go func() {
			if *useRandom {
				rand.Seed(time.Now().UnixNano())
				for {
					seqChan <- rand.Intn(*maxSeqID-*minSeqID) + *minSeqID
				}
			} else {
				for i := *minSeqID; i < *maxSeqID; i++ {
					seqChan <- i
				}
			}
		}()
	}

	c := vitessdriver.Configuration{
		Protocol:  *vtgateconn.VtgateProtocol,
		Address:   *server,
		Target:    *targetString,
		Streaming: *streaming,
	}
	db, err := vitessdriver.OpenWithConfiguration(c)
	if err != nil {
		return nil, fmt.Errorf("client error: %v", err)
	}

	log.Infof("Sending the query...")

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	return execMulti(ctx, db, args[0])
}

func prepareBindVariables() []interface{} {
	bv := make([]interface{}, 0, len(*bindVariables)+1)
	bv = append(bv, (*bindVariables)...)
	if *maxSeqID > *minSeqID {
		bv = append(bv, <-seqChan)
	}
	return bv
}

func execMulti(ctx context.Context, db *sql.DB, sql string) (*results, error) {
	all := newResults()
	ec := concurrency.FirstErrorRecorder{}
	wg := sync.WaitGroup{}
	isDML := sqlparser.IsDML(sql)

	isThrottled := *qps > 0

	start := time.Now()
	for i := 0; i < *parallel; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			var ticker *time.Ticker
			if isThrottled {
				tickDuration := time.Second / time.Duration(*qps)
				ticker = time.NewTicker(tickDuration)
			}

			for j := 0; j < *count; j++ {
				var qr *results
				var err error
				if isDML {
					qr, err = execDml(ctx, db, sql)
				} else {
					qr, err = execNonDml(ctx, db, sql)
				}
				if *count == 1 && *parallel == 1 {
					all = qr
				} else {
					all.merge(qr)
					if err != nil {
						all.recordError(err)
					}
				}
				if err != nil {
					ec.RecordError(err)
					// We keep going and do not return early purpose.
				}

				if ticker != nil {
					<-ticker.C
				}
			}
		}()
	}
	wg.Wait()
	if all != nil {
		all.duration = time.Since(start)
	}

	return all, ec.Error()
}

func execDml(ctx context.Context, db *sql.DB, sql string) (*results, error) {
	start := time.Now()
	tx, err := db.Begin()
	if err != nil {
		return nil, vterrors.Wrap(err, "BEGIN failed")
	}

	result, err := tx.ExecContext(ctx, sql, []interface{}(prepareBindVariables())...)
	if err != nil {
		return nil, vterrors.Wrap(err, "failed to execute DML")
	}

	err = tx.Commit()
	if err != nil {
		return nil, vterrors.Wrap(err, "COMMIT failed")
	}

	rowsAffected, _ := result.RowsAffected()
	lastInsertID, _ := result.LastInsertId()
	return &results{
		rowsAffected: rowsAffected,
		lastInsertID: lastInsertID,
		duration:     time.Since(start),
	}, nil
}

func execNonDml(ctx context.Context, db *sql.DB, sql string) (*results, error) {
	start := time.Now()
	rows, err := db.QueryContext(ctx, sql, []interface{}(prepareBindVariables())...)
	if err != nil {
		return nil, vterrors.Wrap(err, "client error")
	}
	defer vterrors.LogIfError(rows.Close())

	// get the headers
	var qr results
	cols, err := rows.Columns()
	if err != nil {
		return nil, vterrors.Wrap(err, "client error")
	}
	qr.Fields = cols

	// get the rows
	for rows.Next() {
		row := make([]interface{}, len(cols))
		for i := range row {
			var col string
			row[i] = &col
		}
		if err := rows.Scan(row...); err != nil {
			return nil, vterrors.Wrap(err, "client error")
		}

		// unpack []*string into []string
		vals := make([]string, 0, len(row))
		for _, value := range row {
			vals = append(vals, *(value.(*string)))
		}
		qr.Rows = append(qr.Rows, vals)
	}
	qr.rowsAffected = int64(len(qr.Rows))

	if err := rows.Err(); err != nil {
		return nil, vterrors.Wrap(err, "Vitess returned an error")
	}

	qr.duration = time.Since(start)
	return &qr, nil
}

type results struct {
	mu                 sync.Mutex
	Fields             []string   `json:"fields"`
	Rows               [][]string `json:"rows"`
	rowsAffected       int64
	lastInsertID       int64
	duration           time.Duration
	cumulativeDuration time.Duration

	// Multi DML mode: Track total error count, error count per code and the first error.
	totalErrorCount int
	errorCount      map[vtrpcpb.Code]int
	firstError      map[vtrpcpb.Code]error
}

func newResults() *results {
	return &results{
		errorCount: make(map[vtrpcpb.Code]int),
		firstError: make(map[vtrpcpb.Code]error),
	}
}

// merge aggregates "other" into "r".
// This is only used for executing DMLs concurrently and repeatedly.
// Therefore, "Fields" and "Rows" are not merged.
func (r *results) merge(other *results) {
	if other == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.rowsAffected += other.rowsAffected
	if other.lastInsertID > r.lastInsertID {
		r.lastInsertID = other.lastInsertID
	}
	r.cumulativeDuration += other.duration
}

func (r *results) recordError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.totalErrorCount++
	code := vterrors.Code(err)
	r.errorCount[code]++

	if r.errorCount[code] == 1 {
		r.firstError[code] = err
	}
}

func (r *results) print() {
	if r == nil {
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(r.Fields)
	table.SetAutoFormatHeaders(false)
	table.AppendBulk(r.Rows)
	table.Render()
	fmt.Printf("%v row(s) affected (%v, cum: %v)\n", r.rowsAffected, r.duration, r.cumulativeDuration)
	if r.lastInsertID != 0 {
		fmt.Printf("Last insert ID: %v\n", r.lastInsertID)
	}

	if r.totalErrorCount == 0 {
		return
	}

	fmt.Printf("%d error(s) were returned. Number of errors by error code:\n\n", r.totalErrorCount)
	// Sort different error codes by count (descending).
	type errorCounts struct {
		code  vtrpcpb.Code
		count int
	}
	var counts []errorCounts
	for code, count := range r.errorCount {
		counts = append(counts, errorCounts{code, count})
	}
	sort.Slice(counts, func(i, j int) bool { return counts[i].count >= counts[j].count })
	for _, c := range counts {
		fmt.Printf("%- 30v= % 5d\n", c.code, c.count)
	}

	fmt.Printf("\nFirst error per code:\n\n")
	for code, err := range r.firstError {
		fmt.Printf("Code:  %v\nError: %v\n\n", code, err)
	}
}
