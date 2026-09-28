/*
Copyright 2024 The Vitess Authors.

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

package logstats

import (
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type logbv struct {
	Name string
	BVar *querypb.BindVariable
}

// Logger is a zero-allocation logger for logstats.
// It can output logs as JSON or as plaintext, following the commonly used
// logstats format that is shared between the tablets and the gates.
type Logger struct {
	b     []byte
	bvars []logbv
	n     int
	json  bool
}

func sortBVars(sorted []logbv, bvars map[string]*querypb.BindVariable) []logbv {
	for k, bv := range bvars {
		sorted = append(sorted, logbv{k, bv})
	}
	slices.SortFunc(sorted, func(a, b logbv) int {
		return strings.Compare(a.Name, b.Name)
	})
	return sorted
}

func (log *Logger) appendBVarsJSON(b []byte, bvars map[string]*querypb.BindVariable, full bool) []byte {
	log.bvars = sortBVars(log.bvars[:0], bvars)

	b = append(b, '{')
	for i, bv := range log.bvars {
		if i > 0 {
			b = append(b, ',', ' ')
		}
		b = strconv.AppendQuote(b, bv.Name)
		b = append(b, `: {"type": `...)
		b = strconv.AppendQuote(b, querypb.Type_name[int32(bv.BVar.Type)])
		b = append(b, `, "value": `...)

		if sqltypes.IsIntegral(bv.BVar.Type) || sqltypes.IsFloat(bv.BVar.Type) {
			b = append(b, bv.BVar.Value...)
		} else if bv.BVar.Type == sqltypes.Tuple {
			b = append(b, '"')
			b = strconv.AppendInt(b, int64(len(bv.BVar.Values)), 10)
			b = append(b, ` items"`...)
		} else {
			if full {
				b = strconv.AppendQuote(b, hack.String(bv.BVar.Value))
			} else {
				b = append(b, '"')
				b = strconv.AppendInt(b, int64(len(bv.BVar.Values)), 10)
				b = append(b, ` bytes"`...)
			}
		}
		b = append(b, '}')
	}
	return append(b, '}')
}

func (log *Logger) Init(json bool) {
	log.n = 0
	log.json = json
	if log.json {
		log.b = append(log.b, '{')
	}
}

func (log *Logger) Redacted() {
	log.String("[REDACTED]")
}

func (log *Logger) Key(key string) {
	if log.json {
		if log.n > 0 {
			log.b = append(log.b, ',', ' ')
		}
		log.b = append(log.b, '"')
		log.b = append(log.b, key...)
		log.b = append(log.b, '"', ':', ' ')
	} else {
		if log.n > 0 {
			log.b = append(log.b, '\t')
		}
	}
	log.n++
}

func (log *Logger) StringUnquoted(value string) {
	if log.json {
		log.b = strconv.AppendQuote(log.b, value)
	} else {
		log.b = append(log.b, value...)
	}
}

func (log *Logger) TabTerminated() {
	if !log.json {
		log.b = append(log.b, '\t')
	}
}

func (log *Logger) String(value string) {
	log.b = strconv.AppendQuote(log.b, value)
}

func (log *Logger) StringSingleQuoted(value string) {
	if log.json {
		log.b = strconv.AppendQuote(log.b, value)
	} else {
		log.b = append(log.b, '\'')
		log.b = append(log.b, value...)
		log.b = append(log.b, '\'')
	}
}

func (log *Logger) Time(t time.Time) {
	const timeFormat = "2006-01-02 15:04:05.000000"
	if log.json {
		log.b = append(log.b, '"')
		log.b = t.AppendFormat(log.b, timeFormat)
		log.b = append(log.b, '"')
	} else {
		log.b = t.AppendFormat(log.b, timeFormat)
	}
}

func (log *Logger) Duration(t time.Duration) {
	log.b = strconv.AppendFloat(log.b, t.Seconds(), 'f', 6, 64)
}

func (log *Logger) BindVariables(bvars map[string]*querypb.BindVariable, full bool) {
	// the bind variables are printed as JSON in text mode because the original
	// printing syntax, which was simply `fmt.Sprintf("%v")`, is not stable or
	// safe to parse
	log.b = log.appendBVarsJSON(log.b, bvars, full)
}

func (log *Logger) Int(i int64) {
	log.b = strconv.AppendInt(log.b, i, 10)
}

func (log *Logger) Uint(u uint64) {
	log.b = strconv.AppendUint(log.b, u, 10)
}

func (log *Logger) Bool(b bool) {
	log.b = strconv.AppendBool(log.b, b)
}

func (log *Logger) Strings(strs []string) {
	log.b = append(log.b, '[')
	for i, t := range strs {
		if i > 0 {
			log.b = append(log.b, ',')
		}
		log.b = strconv.AppendQuote(log.b, t)
	}
	log.b = append(log.b, ']')
}

func (log *Logger) Flush(w io.Writer) (err error) {
	if log.json {
		log.b = append(log.b, '}')
	}
	log.b = append(log.b, '\n')
	_, err = w.Write(log.b)

	clear(log.bvars)
	log.bvars = log.bvars[:0]
	log.b = log.b[:0]
	log.n = 0

	loggerPool.Put(log)
	return err
}

var loggerPool = sync.Pool{New: func() any {
	return &Logger{}
}}

// NewLogger returns a new Logger instance to perform logstats logging.
// The logger must be initialized with (*Logger).Init before usage and
// flushed with (*Logger).Flush once all the key-values have been written
// to it.
func NewLogger() *Logger {
	return loggerPool.Get().(*Logger)
}
