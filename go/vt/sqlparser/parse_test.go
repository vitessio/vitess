// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/testfiles"
)

func TestGen(t *testing.T) {
	_, err := Parse("select :a from a where a in (:b)")
	if err != nil {
		t.Error(err)
	}
}

func TestParse(t *testing.T) {
	for tcase := range iterateFiles("sqlparser_test/*.sql") {
		if tcase.output == "" {
			tcase.output = tcase.input
		}
		tree, err := Parse(tcase.input)
		var out string
		if err != nil {
			out = err.Error()
		} else {
			out = String(tree)
		}
		if out != tcase.output {
			t.Error(fmt.Sprintf("File:%s Line:%v\n%q\n%q", tcase.file, tcase.lineno, tcase.output, out))
		}
	}
}

func BenchmarkParse1(b *testing.B) {
	sql := "select 'abcd', 20, 30.0, eid from a where 1=eid and name='3'"
	for i := 0; i < b.N; i++ {
		_, err := Parse(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParse2(b *testing.B) {
	sql := "select aaaa, bbb, ccc, ddd, eeee, ffff, gggg, hhhh, iiii from tttt, ttt1, ttt3 where aaaa = bbbb and bbbb = cccc and dddd+1 = eeee group by fff, gggg having hhhh = iiii and iiii = jjjj order by kkkk, llll limit 3, 4"
	for i := 0; i < b.N; i++ {
		_, err := Parse(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

type testCase struct {
	file   string
	lineno int
	input  string
	output string
}

func iterateFiles(pattern string) (testCaseIterator chan testCase) {
	names := testfiles.Glob(pattern)
	testCaseIterator = make(chan testCase)
	go func() {
		defer close(testCaseIterator)
		for _, name := range names {
			fd, err := os.OpenFile(name, os.O_RDONLY, 0)
			if err != nil {
				panic(fmt.Sprintf("Could not open file %s", name))
			}

			r := bufio.NewReader(fd)
			lineno := 0
			for {
				line, err := r.ReadString('\n')
				lines := strings.Split(strings.TrimRight(line, "\n"), "#")
				lineno++
				if err != nil {
					if err != io.EOF {
						panic(fmt.Sprintf("Error reading file %s: %s", name, err.Error()))
					}
					break
				}
				input := lines[0]
				output := ""
				if len(lines) > 1 {
					output = lines[1]
				}
				if input == "" {
					continue
				}
				testCaseIterator <- testCase{name, lineno, input, output}
			}
		}
	}()
	return testCaseIterator
}
