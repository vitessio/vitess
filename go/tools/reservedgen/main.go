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

// Command reservedgen extracts the non_reserved_keyword rule from sql.y, and
// the words MySQL reserves from the keyword list the parser tests are checked
// against, and writes both out as Go sets.
//
// The formatter needs to know which keywords may be written back out bare. The
// positions that take a charset, collation, engine or tablespace name accept an
// identifier or a non-reserved keyword, and nothing else: every other keyword --
// a reserved one, one that is in neither list, or a charset introducer such as
// _utf8mb4 -- lexes as a token those positions reject, so a name that is one has
// to be quoted or the regenerated statement will not parse.
//
// The set is keyed by token rather than by spelling because several spellings
// can share one token, and the tokenizer's keyword table is what maps one to
// the other.
//
// The grammar's idea of non-reserved is not MySQL's, though. Vitess accepts
// `int`, `char`, `array` and a few hundred other words as identifiers that MySQL
// reserves, and a statement that writes one bare is rejected by the MySQL it is
// sent to. The reserved words are therefore a second set, keyed by spelling
// because that is all the list has.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"go/token"
	"os"
	"sort"
	"strings"
)

// minNonReservedKeywords is a canary: the rule has had several hundred entries
// for years, so a small count means the rule's shape changed and the extraction
// went wrong rather than the list shrinking. Failing loudly beats silently
// emitting a short set, which would make the formatter quote names it used to
// write bare.
const minNonReservedKeywords = 300

// minMySQLReservedWords is the same kind of canary for the MySQL keyword list,
// which has had more than 200 reserved words for as long as it has existed.
const minMySQLReservedWords = 200

// MySQLReservedWords returns the lower-cased words the given
// INFORMATION_SCHEMA.KEYWORDS dump marks as reserved, sorted. The dump is
// tab-separated WORD and RESERVED columns, below any number of `//` comment
// lines and a header row.
func MySQLReservedWords(keywords string) ([]string, error) {
	var words []string
	seen := map[string]struct{}{}
	for line := range strings.SplitSeq(keywords, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}
		word, flag, ok := strings.Cut(line, "\t")
		if !ok {
			return nil, fmt.Errorf("unexpected line in the MySQL keyword list: %q", line)
		}
		switch flag {
		case "1":
			word = strings.ToLower(word)
			if _, dup := seen[word]; !dup {
				seen[word] = struct{}{}
				words = append(words, word)
			}
		case "0", "RESERVED":
		default:
			return nil, fmt.Errorf("unexpected line in the MySQL keyword list: %q", line)
		}
	}
	if len(words) < minMySQLReservedWords {
		return nil, fmt.Errorf("only found %d MySQL reserved words, expected at least %d; has the list's shape changed?", len(words), minMySQLReservedWords)
	}
	sort.Strings(words)
	return words, nil
}

// NonReservedKeywords returns the token names listed by sql.y's
// non_reserved_keyword rule, sorted.
func NonReservedKeywords(grammar string) ([]string, error) {
	tokens, err := ruleTokens(grammar, "non_reserved_keyword")
	if err != nil {
		return nil, err
	}
	if len(tokens) < minNonReservedKeywords {
		return nil, fmt.Errorf("only found %d non-reserved keywords, expected at least %d; has the rule's shape changed?", len(tokens), minNonReservedKeywords)
	}
	return tokens, nil
}

// ruleTokens returns the sorted, de-duplicated token names of a grammar rule
// that is a bare alternation of token names, one per line, each optionally
// followed by a %prec clause.
//
// Blank lines and comments inside the rule are skipped. The rule ends at its
// terminating `;`, or -- as sql.y writes its rules without one -- at the next
// rule's header or the `%%` that closes the rules section. Anything else is an
// error, and so is reaching the end of the grammar without finding the rule's
// end: either means the rule does not have the shape this expects, and stopping
// early would silently emit a short list.
func ruleTokens(grammar, rule string) ([]string, error) {
	_, body, found := strings.Cut(grammar, "\n"+rule+":\n")
	if !found {
		return nil, fmt.Errorf("no %s rule found", rule)
	}

	seen := map[string]struct{}{}
	var tokens []string
	inComment := false
	first := true
	for line := range strings.SplitSeq(body, "\n") {
		line, inComment = stripComments(line, inComment)
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if line == "%%" || isRuleHeader(line) {
			return sortTokens(rule, tokens)
		}
		last := strings.HasSuffix(line, ";")
		line = strings.TrimSpace(strings.TrimSuffix(line, ";"))
		if line != "" {
			// Every alternative but the first starts with `|`.
			if after, ok := strings.CutPrefix(line, "|"); ok {
				line = after
			} else if !first {
				return nil, fmt.Errorf("unexpected line in %s rule: %q", rule, line)
			}
			tok, err := entryToken(line)
			if err != nil {
				return nil, fmt.Errorf("unexpected line in %s rule: %w", rule, err)
			}
			first = false
			if _, dup := seen[tok]; !dup {
				seen[tok] = struct{}{}
				tokens = append(tokens, tok)
			}
		}
		if last {
			return sortTokens(rule, tokens)
		}
	}
	return nil, fmt.Errorf("end of %s rule not found", rule)
}

// entryToken returns the token an alternative names. An entry is one token,
// optionally followed by `%prec PRECEDENCE`.
func entryToken(line string) (string, error) {
	fields := strings.Fields(line)
	if len(fields) != 1 && (len(fields) != 3 || fields[1] != "%prec") {
		return "", fmt.Errorf("%q", line)
	}
	// Token names are Go identifiers -- the generated file refers to them as
	// constants -- and most, but not all, are upper case: ExtractValue and
	// UpdateXML are not.
	if !token.IsIdentifier(fields[0]) {
		return "", fmt.Errorf("%q", line)
	}
	return fields[0], nil
}

// stripComments removes the `//` and `/* */` comments from one line of the
// grammar. inComment says whether the line starts inside a block comment, and
// the result says whether the next one does.
func stripComments(line string, inComment bool) (string, bool) {
	var out strings.Builder
	for line != "" {
		if inComment {
			_, rest, closed := strings.Cut(line, "*/")
			if !closed {
				return out.String(), true
			}
			line, inComment = rest, false
			out.WriteByte(' ')
			continue
		}
		i := strings.Index(line, "/*")
		j := strings.Index(line, "//")
		switch {
		case j >= 0 && (i < 0 || j < i):
			out.WriteString(line[:j])
			return out.String(), false
		case i >= 0:
			out.WriteString(line[:i])
			line, inComment = line[i+2:], true
		default:
			out.WriteString(line)
			line = ""
		}
	}
	return out.String(), inComment
}

// isRuleHeader reports whether line starts a new rule, as in `openb:`.
func isRuleHeader(line string) bool {
	name, _, found := strings.Cut(line, ":")
	return found && token.IsIdentifier(name)
}

func sortTokens(rule string, tokens []string) ([]string, error) {
	if len(tokens) == 0 {
		return nil, fmt.Errorf("%s rule is empty", rule)
	}
	sort.Strings(tokens)
	return tokens, nil
}

func generate(tokens, reserved []string) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(`// Code generated by go/tools/reservedgen. DO NOT EDIT.

package sqlparser

// nonReservedKeywords holds the tokens of sql.y's non_reserved_keyword rule.
// A name that lexes as one of these is accepted bare wherever the grammar takes
// sql_id or table_id; a name that lexes as any other keyword is not, and has to
// be quoted -- see isBareName.
var nonReservedKeywords = map[int]struct{}{
`)
	for _, tok := range tokens {
		fmt.Fprintf(&buf, "\t%s: {},\n", tok)
	}
	buf.WriteString(`}

// mysqlReservedWords holds the words MySQL reserves, lower-cased. MySQL rejects
// one of these as a bare name wherever it takes an identifier, whatever the
// grammar here accepts, so a name that is one has to be quoted -- see
// isBareName.
var mysqlReservedWords = map[string]struct{}{
`)
	for _, word := range reserved {
		fmt.Fprintf(&buf, "\t%q: {},\n", word)
	}
	buf.WriteString("}\n")
	return format.Source(buf.Bytes())
}

func main() {
	in := flag.String("in", "sql.y", "path to sql.y")
	mysql := flag.String("mysql", "testdata/mysql_keywords.txt", "path to the MySQL keyword list")
	out := flag.String("out", "non_reserved_keywords.go", "path to the generated file")
	flag.Parse()

	grammar, err := os.ReadFile(*in)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reservedgen: %v\n", err)
		os.Exit(1)
	}
	tokens, err := NonReservedKeywords(string(grammar))
	if err != nil {
		fmt.Fprintf(os.Stderr, "reservedgen: %s: %v\n", *in, err)
		os.Exit(1)
	}
	keywords, err := os.ReadFile(*mysql)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reservedgen: %v\n", err)
		os.Exit(1)
	}
	reserved, err := MySQLReservedWords(string(keywords))
	if err != nil {
		fmt.Fprintf(os.Stderr, "reservedgen: %s: %v\n", *mysql, err)
		os.Exit(1)
	}
	src, err := generate(tokens, reserved)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reservedgen: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, src, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "reservedgen: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("reservedgen: wrote %d non-reserved keywords and %d MySQL reserved words to %s\n", len(tokens), len(reserved), *out)
}
