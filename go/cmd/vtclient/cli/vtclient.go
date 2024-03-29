/*
Copyright 2023 The Vitess Authors.

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

package cli

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/grpccommon"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vitessdriver"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	// Include deprecation warnings for soon-to-be-unsupported flag invocations.
)

var (
	server        string
	streaming     bool
	targetString  string
	jsonOutput    bool
	useRandom     bool
	bindVariables *bindvars

	timeout  = 30 * time.Second
	parallel = 1
	count    = 1
	minSeqID int
	maxSeqID int
	qps      int

	Main = &cobra.Command{
		Use:   "vtclient <query>",
		Short: "vtclient connects to a vtgate server using the standard go driver API.",
		Long: `vtclient connects to a vtgate server using the standard go driver API.

For query bound variables, we assume place-holders in the query string
in the form of :v1, :v2, etc.`,
		Example: `vtclient --server vtgate:15991 "SELECT * FROM messages"

vtclient --server vtgate:15991 --target '@primary' --bind_variables '[ 12345, 1, "msg 12345" ]' "INSERT INTO messages (page,time_created_ns,message) VALUES (:v1, :v2, :v3)"`,
		Args:    cobra.ExactArgs(1),
		Version: servenv.AppVersion.String(),
		RunE:    run,
	}
)

var (
	seqChan = make(chan int, 10)
)

func init() {
	servenv.MoveFlagsToCobraCommand(Main)

	Main.Flags().StringVar(&server, "server", server, "vtgate server to connect to")
	Main.Flags().DurationVar(&timeout, "timeout", timeout, "timeout for queries")
	Main.Flags().BoolVar(&streaming, "streaming", streaming, "use a streaming query")
	Main.Flags().StringVar(&targetString, "target", targetString, "keyspace:shard@tablet_type")
	Main.Flags().BoolVar(&jsonOutput, "json", jsonOutput, "Output JSON instead of human-readable table")
	Main.Flags().IntVar(&parallel, "parallel", parallel, "DMLs only: Number of threads executing the same query in parallel. Useful for simple load testing.")
	Main.Flags().IntVar(&count, "count", count, "DMLs only: Number of times each thread executes the query. Useful for simple, sustained load testing.")
	Main.Flags().IntVar(&minSeqID, "min_sequence_id", minSeqID, "min sequence ID to generate. When max_sequence_id > min_sequence_id, for each query, a number is generated in [min_sequence_id, max_sequence_id) and attached to the end of the bind variables.")
	Main.Flags().IntVar(&maxSeqID, "max_sequence_id", maxSeqID, "max sequence ID.")
	Main.Flags().BoolVar(&useRandom, "use_random_sequence", useRandom, "use random sequence for generating [min_sequence_id, max_sequence_id)")
	Main.Flags().IntVar(&qps, "qps", qps, "queries per second to throttle each thread at.")

	acl.RegisterFlags(Main.Flags())
	grpccommon.RegisterFlags(Main.Flags())
	servenv.RegisterMySQLServerFlags(Main.Flags())

	bindVariables = newBindvars(Main.Flags(), "bind_variables", "bind variables as a json list")
}

type bindvars []any

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
func (bv *bindvars) Get() any {
	return bv
}

// Type is part of the pflag.Value interface. bindvars.Set() expects all numbers as float64.
func (bv *bindvars) Type() string {
	return "float64"
}

func newBindvars(fs *pflag.FlagSet, name, usage string) *bindvars {
	var bv bindvars
	fs.Var(&bv, name, usage)
	return &bv
}

func run(cmd *cobra.Command, args []string) error {
	defer logutil.Flush()

	qr, err := _run(cmd, args)
	if jsonOutput && qr != nil {
		data, err := json.MarshalIndent(qr, "", "  ")
		if err != nil {
			return fmt.Errorf("cannot marshal data: %w", err)
		}
		fmt.Fprint(cmd.OutOrStdout(), string(data))
		return nil
	}

	qr.print(cmd.OutOrStdout())
	return err
}

func _run(cmd *cobra.Command, args []string) (*results, error) {
	logutil.PurgeLogs()

	if maxSeqID > minSeqID {
		go func() {
			if useRandom {
				for {
					seqChan <- rand.IntN(maxSeqID-minSeqID) + minSeqID
				}
			} else {
				for i := minSeqID; i < maxSeqID; i++ {
					seqChan <- i
				}
			}
		}()
	}

	c := vitessdriver.Configuration{
		Protocol:  vtgateconn.GetVTGateProtocol(),
		Address:   server,
		Target:    targetString,
		Streaming: streaming,
	}
	db, err := vitessdriver.OpenWithConfiguration(c)
	if err != nil {
		return nil, fmt.Errorf("client error: %w", err)
	}

	log.Infof("Sending the query...")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return execMulti(ctx, db, cmd.Flags().Arg(0))
}

func prepareBindVariables() []any {
	bv := make([]any, 0, len(*bindVariables)+1)
	bv = append(bv, (*bindVariables)...)
	if maxSeqID > minSeqID {
		bv = append(bv, <-seqChan)
	}
	return bv
}

func execMulti(ctx context.Context, db *sql.DB, sql string) (*results, error) {
	all := newResults()
	ec := concurrency.FirstErrorRecorder{}
	wg := sync.WaitGroup{}
	isDML := sqlparser.IsDML(sql)

	isThrottled := qps > 0

	start := time.Now()
	for i := 0; i < parallel; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			var ticker *time.Ticker
			if isThrottled {
				tickDuration := time.Second / time.Duration(qps)
				ticker = time.NewTicker(tickDuration)
			}

			for j := 0; j < count; j++ {
				var qr *results
				var err error
				if isDML {
					qr, err = execDml(ctx, db, sql)
				} else {
					qr, err = execNonDml(ctx, db, sql)
				}
				if count == 1 && parallel == 1 {
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

	result, err := tx.ExecContext(ctx, sql, []any(prepareBindVariables())...)
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
	rows, err := db.QueryContext(ctx, sql, []any(prepareBindVariables())...)
	if err != nil {
		return nil, vterrors.Wrap(err, "client error")
	}
	defer rows.Close()

	// get the headers
	var qr results
	cols, err := rows.Columns()
	if err != nil {
		return nil, vterrors.Wrap(err, "client error")
	}
	qr.Fields = cols

	// get the rows
	for rows.Next() {
		row := make([]any, len(cols))
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

func (r *results) print(w io.Writer) {
	if r == nil {
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(r.Fields)
	table.SetAutoFormatHeaders(false)
	table.AppendBulk(r.Rows)
	table.Render()
	fmt.Fprintf(w, "%v row(s) affected (%v, cum: %v)\n", r.rowsAffected, r.duration, r.cumulativeDuration)
	if r.lastInsertID != 0 {
		fmt.Fprintf(w, "Last insert ID: %v\n", r.lastInsertID)
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
		fmt.Fprintf(w, "%- 30v= % 5d\n", c.code, c.count)
	}

	fmt.Fprintf(w, "\nFirst error per code:\n\n")
	for code, err := range r.firstError {
		fmt.Fprintf(w, "Code:  %v\nError: %v\n\n", code, err)
	}
}
