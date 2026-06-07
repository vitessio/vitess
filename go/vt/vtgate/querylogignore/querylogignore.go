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

// Package querylogignore implements an opt-in allow/deny mechanism for
// suppressing specific SQL query shapes from the vtgate query log.
//
// Operators configure a list of patterns via --query-log-ignore-patterns.
// Each pattern is matched against the normalized shape of an incoming query
// (literals replaced with bind placeholders, e.g. "select :vtg1"), with a
// case-insensitive trimmed raw-string fallback for queries that don't parse.
package querylogignore

import (
	"log/slog"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"vitess.io/vitess/go/viperutil"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
)

// filePrefix marks the flag value as a path to a file containing one pattern
// per line (blank lines and lines starting with "#" are skipped).
const filePrefix = "@"

// typeCommentRE strips the " /* INT64 */"-style type annotations that
// sqlparser.Normalize attaches to each replaced literal. Without this,
// patterns supplied in bind-variable form would not match queries whose
// literals were normalized away.
var typeCommentRE = regexp.MustCompile(` /\* [A-Z0-9_]+ \*/`)

// bindVarRE collapses any bind-variable reference (`:vtg1`, `:redacted1`,
// `:x`, ...) to a single placeholder so that patterns written in any of
// the conventional bind-variable styles compare equal.
var bindVarRE = regexp.MustCompile(`:[a-zA-Z_][a-zA-Z0-9_]*`)

// canonicalize reduces a redacted SQL string to a form that ignores both
// type annotations and the specific bind-variable names chosen by the
// normalizer, so equivalent shapes match regardless of how the operator
// wrote the pattern.
func canonicalize(redacted string) string {
	redacted = typeCommentRE.ReplaceAllString(redacted, "")
	redacted = bindVarRE.ReplaceAllString(redacted, ":_")
	return redacted
}

// IgnoreSet is the parsed form of --query-log-ignore-patterns. It contains
// both normalized and raw-fallback forms of every configured pattern so a
// single map lookup per tier answers ShouldIgnore.
type IgnoreSet struct {
	// set holds the normalized form of each parseable pattern, plus the
	// trimmed-lowercased raw form of patterns that fail to parse. The empty
	// case is the common one and is detected via len(set) == 0 on the hot
	// path.
	set map[string]struct{}

	// source preserves the original flag value so the viper GetFunc can
	// short-circuit re-parsing when the underlying string hasn't changed.
	source string
}

// NewIgnoreSet builds an IgnoreSet from the raw flag value. The value is
// either a comma-separated list of patterns or, if it begins with "@", a
// path to a file containing one pattern per line. Patterns that fail to
// parse are stored as their trimmed lower-cased raw form, so unparseable
// queries like "select $$" can still be matched.
func NewIgnoreSet(rawValue string, parser *sqlparser.Parser) *IgnoreSet {
	s := &IgnoreSet{source: rawValue}

	patterns := loadPatterns(rawValue)
	if len(patterns) == 0 {
		return s
	}

	s.set = make(map[string]struct{}, len(patterns))
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if parser != nil {
			if normalized, err := parser.RedactSQLQuery(p); err == nil {
				s.set[canonicalize(normalized)] = struct{}{}
				continue
			}
		}
		s.set[strings.ToLower(p)] = struct{}{}
	}
	return s
}

// loadPatterns returns the raw pattern strings from the flag value, reading
// from disk when the value begins with "@". A read error is logged and
// returns an empty slice so vtgate still starts.
func loadPatterns(rawValue string) []string {
	rawValue = strings.TrimSpace(rawValue)
	if rawValue == "" {
		return nil
	}
	if strings.HasPrefix(rawValue, filePrefix) {
		path := rawValue[len(filePrefix):]
		data, err := os.ReadFile(path)
		if err != nil {
			log.Warn(
				"query-log-ignore-patterns: failed to read file; ignore-list will be empty",
				slog.String("path", path),
				slog.Any("error", err),
			)
			return nil
		}
		var out []string
		for line := range strings.SplitSeq(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			out = append(out, line)
		}
		return out
	}
	return strings.Split(rawValue, ",")
}

// ShouldIgnore reports whether the given SQL string matches any configured
// pattern. The empty-set check is a single integer comparison so operators
// who do not configure the flag pay essentially no cost.
func (s *IgnoreSet) ShouldIgnore(sql string, parser *sqlparser.Parser) bool {
	if s == nil || len(s.set) == 0 {
		return false
	}
	if parser != nil {
		if normalized, err := parser.RedactSQLQuery(sql); err == nil {
			if _, ok := s.set[canonicalize(normalized)]; ok {
				return true
			}
		}
	}
	raw := strings.ToLower(strings.TrimSpace(sql))
	_, ok := s.set[raw]
	return ok
}

// Source returns the original flag string used to build this IgnoreSet.
func (s *IgnoreSet) Source() string {
	if s == nil {
		return ""
	}
	return s.source
}

// String implements fmt.Stringer so that viper's GetString returns the
// underlying source. The dynamic GetFunc relies on this when comparing the
// freshly-read config value against the cached IgnoreSet's source.
func (s *IgnoreSet) String() string {
	return s.Source()
}

// flagParser is used to normalize patterns at flag-parse time. The executor
// passes its own parser at lookup time; this one only handles the small set
// of configured patterns, so the default MySQL server version is sufficient.
var flagParser = mustNewParser()

func mustNewParser() *sqlparser.Parser {
	p, err := sqlparser.New(sqlparser.Options{})
	if err != nil {
		log.Error(
			"query-log-ignore-patterns: failed to construct flag parser; parseable shape matching disabled",
			slog.Any("error", err),
		)
		return nil
	}
	return p
}

// ignoreSetCache caches the most recently parsed *IgnoreSet keyed by the
// raw config string. This avoids re-parsing patterns (and re-reading
// @file contents) on every call to IgnorePatterns.Get() in the query-log
// hot path.
//
// For @file values, only the flag string (e.g. "@/etc/patterns.txt") is
// the cache key — file content changes require a config reload or process
// restart to take effect.
var ignoreSetCache struct {
	mu     sync.Mutex
	rawVal string
	parsed *IgnoreSet
}

func cachedIgnoreSet(rawVal string) *IgnoreSet {
	ignoreSetCache.mu.Lock()
	defer ignoreSetCache.mu.Unlock()

	if ignoreSetCache.parsed != nil && ignoreSetCache.rawVal == rawVal {
		return ignoreSetCache.parsed
	}

	parsed := NewIgnoreSet(rawVal, flagParser)
	ignoreSetCache.rawVal = rawVal
	ignoreSetCache.parsed = parsed
	return parsed
}

// IgnorePatterns is the Viper-managed flag value backing
// --query-log-ignore-patterns. Empty by default. Reloadable via Viper.
var IgnorePatterns = viperutil.Configure(
	"query_log_ignore_patterns",
	viperutil.Options[*IgnoreSet]{
		FlagName: "query-log-ignore-patterns",
		Default:  &IgnoreSet{},
		Dynamic:  true,
		GetFunc: func(v *viper.Viper) func(key string) *IgnoreSet {
			return func(key string) *IgnoreSet {
				return cachedIgnoreSet(v.GetString(key))
			}
		},
	},
)

// RegisterFlags installs --query-log-ignore-patterns on the given FlagSet.
func RegisterFlags(fs *pflag.FlagSet) {
	fs.String(
		"query-log-ignore-patterns",
		"",
		"Comma-separated list of SQL query shapes to suppress from the vtgate query log. "+
			"Patterns are matched against the normalized query shape (literals replaced with bind placeholders, "+
			"e.g. 'select :vtg1'); unparseable patterns fall back to case-insensitive trimmed raw-string match. "+
			"Prefix the value with '@' to read patterns from a file (one per line; '#' starts a comment). "+
			"Empty by default.",
	)
	viperutil.BindFlags(fs, IgnorePatterns)
}

func init() {
	for _, cmd := range []string{"vtgate", "vtcombo"} {
		servenv.OnParseFor(cmd, RegisterFlags)
	}
}
