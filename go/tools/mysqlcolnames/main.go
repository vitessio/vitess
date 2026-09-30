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

// Command mysqlcolnames records the result-set column names that a real MySQL
// server returns for every case in the column-name corpus
// (go/vt/sqlparser/testdata/column_names.json), and writes them back into the
// corpus. The corpus is the reference that Vitess's column naming is tested
// against.
//
// Usage:
//
//	go run ./go/tools/mysqlcolnames --port 3306 --target mysql84
//	go run ./go/tools/mysqlcolnames --port 3307 --target mysql80 --baseline mysql84
//
// Each case runs on a fresh connection to the database named by --database,
// which is recreated from --schema first. Cases with "params" are run as
// prepared statements, and all other cases over the text protocol.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"reflect"
	"slices"
	"strings"
	"unicode/utf8"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/sqlerror"
)

// Case is one corpus entry.
type Case struct {
	ID       string   `json:"id"`
	Category string   `json:"category"`
	Setup    []string `json:"setup,omitempty"`
	Query    string   `json:"query"`
	Params   []any    `json:"params,omitempty"`
	Cleanup  []string `json:"cleanup,omitempty"`

	// Results holds one Result per MySQL version, keyed by the --target name.
	Results map[string]*Result `json:"-"`
}

// Result is what one MySQL server returned for a case.
type Result struct {
	// Names are the column names. A name that is not valid UTF-8 has its
	// invalid bytes replaced, and its exact bytes are in NamesHex.
	Names []string `json:"names,omitempty"`
	// NamesHex holds the hex-encoded bytes of every name, only when at least
	// one name is not valid UTF-8.
	NamesHex []string `json:"names_hex,omitempty"`
	// Error is set when the query fails, as "<error number>: <message>".
	Error string `json:"error,omitempty"`
	// SetupErrors lists the setup statements that failed. Some cases exist to
	// record such a failure, for example CREATE TABLE ... SELECT with a
	// generated column name that MySQL rejects.
	SetupErrors []string `json:"setup_errors,omitempty"`
}

var (
	file     = pflag.String("file", "go/vt/sqlparser/testdata/column_names.json", "corpus file to read and update")
	schema   = pflag.String("schema", "go/vt/sqlparser/testdata/column_names_schema.sql", "schema to load before running the cases")
	host     = pflag.String("host", "127.0.0.1", "MySQL host")
	port     = pflag.Int("port", 3306, "MySQL port")
	user     = pflag.String("user", "root", "MySQL user")
	password = pflag.String("password", "", "MySQL password")
	database = pflag.String("database", "colnames", "database the schema is loaded into and the cases run in")
	target   = pflag.String("target", "mysql84", "key under which the results are stored in the corpus")
	baseline = pflag.String("baseline", "", "if set, results equal to this key's results are not stored")
	only     = pflag.String("only", "", "only run cases whose id starts with this prefix")
)

func main() {
	pflag.Parse()
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cases, err := readCorpus(*file)
	if err != nil {
		return err
	}
	if err := loadSchema(ctx); err != nil {
		return err
	}

	cfg := gomysql.NewConfig()
	cfg.User = *user
	cfg.Passwd = *password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", *host, *port)
	cfg.DBName = *database
	cfg.InterpolateParams = false
	cfg.Params = map[string]string{"charset": "utf8mb4"}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxIdleConns(0)

	ran := 0
	for _, c := range cases {
		if !strings.HasPrefix(c.ID, *only) {
			continue
		}
		var res *Result
		if len(c.Params) > 0 {
			res = runPrepared(ctx, db, c)
		} else {
			res = runText(ctx, c)
		}
		if *baseline != "" && reflect.DeepEqual(res, c.Results[*baseline]) {
			delete(c.Results, *target)
		} else {
			c.Results[*target] = res
		}
		ran++
	}
	fmt.Fprintf(os.Stderr, "ran %d cases\n", ran)
	return writeCorpus(*file, cases)
}

func connParams(dbName string) *mysql.ConnParams {
	return &mysql.ConnParams{
		Host:    *host,
		Port:    *port,
		Uname:   *user,
		Pass:    *password,
		DbName:  dbName,
		Charset: collations.CollationUtf8mb4ID,
	}
}

func loadSchema(ctx context.Context) error {
	data, err := os.ReadFile(*schema)
	if err != nil {
		return err
	}
	conn, err := mysql.Connect(ctx, connParams(""))
	if err != nil {
		return err
	}
	defer conn.Close()
	for stmt := range strings.SplitSeq(string(data), ";\n") {
		if strings.TrimSpace(stripComments(stmt)) == "" {
			continue
		}
		if _, err := conn.ExecuteFetch(stmt, 0, false); err != nil {
			return fmt.Errorf("loading schema: %q: %w", stmt, err)
		}
	}
	return nil
}

func stripComments(stmt string) string {
	var b strings.Builder
	for line := range strings.SplitSeq(stmt, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			b.WriteString(line)
			b.WriteByte('\n')
		}
	}
	return b.String()
}

func runText(ctx context.Context, c *Case) *Result {
	conn, err := mysql.Connect(ctx, connParams(*database))
	if err != nil {
		return &Result{Error: "connect: " + err.Error()}
	}
	defer conn.Close()
	var setupErrors []string
	for _, stmt := range c.Setup {
		if _, err := conn.ExecuteFetch(stmt, 10000, false); err != nil {
			setupErrors = append(setupErrors, errorString(err))
		}
	}
	defer func() {
		for _, stmt := range c.Cleanup {
			_, _ = conn.ExecuteFetch(stmt, 10000, false)
		}
	}()
	qr, err := conn.ExecuteFetch(c.Query, 10000, true)
	if err != nil {
		return &Result{Error: errorString(err), SetupErrors: setupErrors}
	}
	var names []string
	for _, f := range qr.Fields {
		names = append(names, f.Name)
	}
	res := newResult(names)
	res.SetupErrors = setupErrors
	return res
}

func runPrepared(ctx context.Context, db *sql.DB, c *Case) *Result {
	conn, err := db.Conn(ctx)
	if err != nil {
		return &Result{Error: "connect: " + err.Error()}
	}
	defer conn.Close()
	var setupErrors []string
	for _, stmt := range c.Setup {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			setupErrors = append(setupErrors, errorString(err))
		}
	}
	defer func() {
		for _, stmt := range c.Cleanup {
			_, _ = conn.ExecContext(ctx, stmt)
		}
	}()
	args := make([]any, 0, len(c.Params))
	for _, p := range c.Params {
		// JSON numbers decode as float64. Bind whole numbers as integers, the
		// way a client would.
		if f, ok := p.(float64); ok && f == float64(int64(f)) {
			p = int64(f)
		}
		args = append(args, p)
	}
	rows, err := conn.QueryContext(ctx, c.Query, args...)
	if err != nil {
		return &Result{Error: errorString(err), SetupErrors: setupErrors}
	}
	defer rows.Close()
	names, err := rows.Columns()
	if err != nil {
		return &Result{Error: errorString(err), SetupErrors: setupErrors}
	}
	for rows.Next() {
	}
	res := newResult(names)
	res.SetupErrors = setupErrors
	return res
}

// newResult returns the result for the given names. A statement without a
// result set, such as SELECT ... INTO, has no names.
func newResult(names []string) *Result {
	res := &Result{Names: names}
	for _, name := range names {
		if !utf8.ValidString(name) {
			res.Names = make([]string, len(names))
			res.NamesHex = make([]string, len(names))
			for i, n := range names {
				res.Names[i] = strings.ToValidUTF8(n, "�")
				res.NamesHex[i] = hex.EncodeToString([]byte(n))
			}
			break
		}
	}
	return res
}

func errorString(err error) string {
	var myErr *gomysql.MySQLError
	if errors.As(err, &myErr) {
		return fmt.Sprintf("%d: %s", myErr.Number, myErr.Message)
	}
	if sqlErr, ok := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError); ok {
		return fmt.Sprintf("%d: %s", sqlErr.Number(), sqlErr.Message)
	}
	return err.Error()
}

// readCorpus reads the corpus. Result keys are any keys other than the
// case-definition fields.
func readCorpus(path string) ([]*Case, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw []map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	cases := make([]*Case, 0, len(raw))
	for _, entry := range raw {
		c := &Case{Results: map[string]*Result{}}
		for key, value := range entry {
			var err error
			switch key {
			case "id":
				err = json.Unmarshal(value, &c.ID)
			case "category":
				err = json.Unmarshal(value, &c.Category)
			case "setup":
				err = json.Unmarshal(value, &c.Setup)
			case "query":
				err = json.Unmarshal(value, &c.Query)
			case "params":
				err = json.Unmarshal(value, &c.Params)
			case "cleanup":
				err = json.Unmarshal(value, &c.Cleanup)
			default:
				res := &Result{}
				err = json.Unmarshal(value, res)
				c.Results[key] = res
			}
			if err != nil {
				return nil, fmt.Errorf("parsing %s: %s: %w", path, key, err)
			}
		}
		cases = append(cases, c)
	}
	return cases, nil
}

// writeCorpus writes the corpus as a JSON array with one case per line, so
// that changes are easy to review.
func writeCorpus(path string, cases []*Case) error {
	var buf bytes.Buffer
	buf.WriteString("[\n")
	for i, c := range cases {
		line, err := marshalCase(c)
		if err != nil {
			return err
		}
		buf.Write(line)
		if i < len(cases)-1 {
			buf.WriteByte(',')
		}
		buf.WriteByte('\n')
	}
	buf.WriteString("]\n")
	return os.WriteFile(path, buf.Bytes(), 0o644)
}

func marshalCase(c *Case) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	first := true
	field := func(key string, value any) error {
		var b bytes.Buffer
		enc := json.NewEncoder(&b)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(value); err != nil {
			return err
		}
		if !first {
			buf.WriteString(", ")
		}
		first = false
		fmt.Fprintf(&buf, "%q: ", key)
		buf.Write(bytes.TrimRight(b.Bytes(), "\n"))
		return nil
	}
	fields := []struct {
		key   string
		value any
		keep  bool
	}{
		{"id", c.ID, true},
		{"category", c.Category, true},
		{"setup", c.Setup, len(c.Setup) > 0},
		{"query", c.Query, true},
		{"params", c.Params, len(c.Params) > 0},
		{"cleanup", c.Cleanup, len(c.Cleanup) > 0},
	}
	for _, f := range fields {
		if f.keep {
			if err := field(f.key, f.value); err != nil {
				return nil, err
			}
		}
	}
	for _, key := range []string{"mysql84", "mysql80"} {
		if res, ok := c.Results[key]; ok {
			if err := field(key, res); err != nil {
				return nil, err
			}
		}
	}
	for _, key := range slices.Sorted(maps.Keys(c.Results)) {
		if key != "mysql84" && key != "mysql80" {
			if err := field(key, c.Results[key]); err != nil {
				return nil, err
			}
		}
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}
