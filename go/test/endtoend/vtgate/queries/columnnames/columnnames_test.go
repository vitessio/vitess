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

package columnnames

import (
	"bufio"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

const knownDivergencesFile = "known_divergences.txt"

type corpusCase struct {
	ID       string   `json:"id"`
	Category string   `json:"category"`
	Setup    []string `json:"setup"`
	Query    string   `json:"query"`
	Params   []any    `json:"params"`
	Cleanup  []string `json:"cleanup"`
	MySQL84  *result  `json:"mysql84"`
	MySQL80  *result  `json:"mysql80"`
}

type result struct {
	Names    []string `json:"names"`
	NamesHex []string `json:"names_hex"`
	Error    string   `json:"error"`
}

// expected returns the result MySQL gave for the tablets' MySQL version.
func (c *corpusCase) expected() *result {
	if mysqlVersionKey == "mysql80" && c.MySQL80 != nil {
		return c.MySQL80
	}
	return c.MySQL84
}

// skipped reports why a case is not run against vtgate, or "" if it is run.
func (c *corpusCase) skipped() string {
	if c.Category == "show" {
		return "SHOW statements are out of scope"
	}
	for _, stmt := range c.Setup {
		lower := strings.ToLower(stmt)
		if strings.HasPrefix(lower, "create ") || strings.HasPrefix(lower, "drop ") || strings.HasPrefix(lower, "insert ") {
			return "the setup changes the schema"
		}
	}
	return ""
}

type target struct {
	keyspace string
	mode     string // "oltp", "olap" or "prepared"
}

func (tg target) String() string {
	short := "sharded"
	if tg.keyspace == unshardedKs {
		short = "unsharded"
	}
	return short + "/" + tg.mode
}

var targets = []target{
	{shardedKs, "oltp"},
	{shardedKs, "olap"},
	{shardedKs, "prepared"},
	{unshardedKs, "oltp"},
	{unshardedKs, "olap"},
	{unshardedKs, "prepared"},
}

// TestColumnNames runs every corpus case against vtgate and checks that the
// column names match MySQL's. Divergences that are not fixed yet are listed in
// known_divergences.txt. A listed divergence that now matches fails the test
// too, so that the list only ever shrinks. Run with -update-known to rewrite
// the list.
func TestColumnNames(t *testing.T) {
	cases := readCorpus(t)
	known := readKnownDivergences(t)
	var found []string

	for _, tg := range targets {
		t.Run(tg.String(), func(t *testing.T) {
			for _, c := range cases {
				if c.skipped() != "" {
					continue
				}
				if len(c.Params) > 0 && tg.mode != "prepared" {
					// Placeholders only exist in prepared statements.
					continue
				}
				want := c.expected()
				got := run(t, tg, c)
				key := c.ID + " " + tg.String()
				if matches(want, got) {
					if known[key] && !*updateKnown {
						t.Errorf("%s: %q now matches MySQL; remove %q from %s", c.ID, c.Query, key, knownDivergencesFile)
					}
					continue
				}
				found = append(found, key)
				switch {
				case !known[key] && !*updateKnown:
					t.Errorf("%s: %q: MySQL returns %s, vtgate returns %s", c.ID, c.Query, describe(want), describe(got))
				case *showKnown:
					t.Logf("%s: %q: MySQL returns %s, vtgate returns %s", c.ID, c.Query, describe(want), describe(got))
				}
			}
		})
	}

	if *updateKnown {
		writeKnownDivergences(t, found)
	}
}

func readCorpus(t *testing.T) []*corpusCase {
	data, err := os.ReadFile(corpusFile)
	require.NoError(t, err)
	var cases []*corpusCase
	require.NoError(t, json.Unmarshal(data, &cases))
	require.NotEmpty(t, cases)
	return cases
}

func readKnownDivergences(t *testing.T) map[string]bool {
	known := map[string]bool{}
	f, err := os.Open(knownDivergencesFile)
	if os.IsNotExist(err) {
		return known
	}
	require.NoError(t, err)
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		known[line] = true
	}
	require.NoError(t, scanner.Err())
	return known
}

func writeKnownDivergences(t *testing.T, found []string) {
	slices.Sort(found)
	var b strings.Builder
	b.WriteString("# Corpus cases whose column names vtgate does not return like MySQL yet.\n")
	b.WriteString("# Format: <case id> <keyspace>/<mode>. Regenerate with -update-known.\n")
	for _, key := range found {
		b.WriteString(key)
		b.WriteByte('\n')
	}
	require.NoError(t, os.WriteFile(knownDivergencesFile, []byte(b.String()), 0o644))
}

// matches reports whether vtgate returned what MySQL returned: the same names,
// byte for byte, or an error when MySQL returned an error.
func matches(want, got *result) bool {
	if want.Error != "" || got.Error != "" {
		return want.Error != "" && got.Error != ""
	}
	return slices.Equal(namesHex(want), namesHex(got))
}

func namesHex(r *result) []string {
	if r.NamesHex != nil {
		return r.NamesHex
	}
	out := make([]string, 0, len(r.Names))
	for _, name := range r.Names {
		out = append(out, hex.EncodeToString([]byte(name)))
	}
	return out
}

func describe(r *result) string {
	if r.Error != "" {
		return "error " + r.Error
	}
	out, _ := json.Marshal(r.Names)
	return string(out)
}

var corpusDBRe = regexp.MustCompile(`\b` + corpusDB + `\b`)

// run executes a case against vtgate and returns the column names. The corpus
// refers to the database "colnames", which becomes the keyspace name, and the
// keyspace name in returned names becomes "colnames" again.
func run(t *testing.T, tg target, c *corpusCase) *result {
	toKs := func(s string) string { return corpusDBRe.ReplaceAllString(s, tg.keyspace) }
	var res *result
	if tg.mode == "prepared" {
		res = runPrepared(t, tg, c, toKs)
	} else {
		res = runText(t, tg, c, toKs)
	}
	for i, name := range res.Names {
		res.Names[i] = strings.ReplaceAll(name, tg.keyspace, corpusDB)
	}
	return res
}

func runText(t *testing.T, tg target, c *corpusCase, toKs func(string) string) *result {
	params := vtParams
	params.DbName = tg.keyspace
	conn, err := mysql.Connect(t.Context(), &params)
	require.NoError(t, err)
	defer conn.Close()

	if tg.mode == "olap" {
		_, err := conn.ExecuteFetch("set workload = olap", 0, false)
		require.NoError(t, err)
	}
	for _, stmt := range c.Setup {
		if _, err := conn.ExecuteFetch(toKs(stmt), 10000, false); err != nil {
			return &result{Error: "setup: " + err.Error()}
		}
	}
	defer func() {
		for _, stmt := range c.Cleanup {
			_, _ = conn.ExecuteFetch(toKs(stmt), 10000, false)
		}
	}()
	qr, err := conn.ExecuteFetch(toKs(c.Query), 10000, true)
	if err != nil {
		return &result{Error: err.Error()}
	}
	res := &result{}
	for _, f := range qr.Fields {
		res.Names = append(res.Names, f.Name)
	}
	return res
}

func runPrepared(t *testing.T, tg target, c *corpusCase, toKs func(string) string) *result {
	cfg := gomysql.NewConfig()
	cfg.User = vtParams.Uname
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", vtParams.Host, vtParams.Port)
	cfg.DBName = tg.keyspace
	cfg.InterpolateParams = false
	cfg.Params = map[string]string{"charset": "utf8mb4"}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer db.Close()

	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	for _, stmt := range c.Setup {
		if _, err := conn.ExecContext(t.Context(), toKs(stmt)); err != nil {
			return &result{Error: "setup: " + err.Error()}
		}
	}
	defer func() {
		for _, stmt := range c.Cleanup {
			_, _ = conn.ExecContext(t.Context(), toKs(stmt))
		}
	}()

	args := make([]any, 0, len(c.Params))
	for _, p := range c.Params {
		if f, ok := p.(float64); ok && f == float64(int64(f)) {
			p = int64(f)
		}
		args = append(args, p)
	}
	// Prepare explicitly: without arguments, QueryContext would use the text
	// protocol.
	stmt, err := conn.PrepareContext(t.Context(), toKs(c.Query))
	if err != nil {
		return &result{Error: err.Error()}
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(t.Context(), args...)
	if err != nil {
		return &result{Error: err.Error()}
	}
	defer rows.Close()
	names, err := rows.Columns()
	if err != nil {
		return &result{Error: err.Error()}
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		return &result{Error: err.Error()}
	}
	return &result{Names: names}
}
