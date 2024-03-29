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

package testcases

import (
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type Query func(query string, row []sqltypes.Value)
type Runner func(yield Query)
type TestCase struct {
	Run     Runner
	Schema  []*querypb.Field
	Compare *Comparison
}

func (tc TestCase) Name() string {
	ptr := reflect.ValueOf(tc.Run).Pointer()
	name := runtime.FuncForPC(ptr).Name()
	return name[strings.LastIndexByte(name, '.')+1:]
}

func perm(a []string, f func([]string)) {
	perm1(a, f, 0)
}

func perm1(a []string, f func([]string), i int) {
	if i > len(a) {
		f(a)
		return
	}
	perm1(a, f, i+1)
	for j := i + 1; j < len(a); j++ {
		a[i], a[j] = a[j], a[i]
		perm1(a, f, i+1)
		a[i], a[j] = a[j], a[i]
	}
}

func genSubsets1(args []string, subset []string, a, b int, yield func([]string)) {
	if a == len(subset) {
		yield(subset)
		return
	}
	if b >= len(args) {
		return
	}
	subset[a] = args[b]
	genSubsets1(args, subset, a+1, b+1, yield)
	genSubsets1(args, subset, a+0, b+1, yield)
}

func genSubsets(args []string, subsetLen int, yield func([]string)) {
	subset := make([]string, subsetLen)
	genSubsets1(args, subset, 0, 0, yield)
}

func mustJSON(j string) sqltypes.Value {
	v, err := sqltypes.NewJSON(j)
	if err != nil {
		panic(err)
	}
	return v
}

type bugs struct{}

// CanCompare skips comparisons in CASE and IN expressions that behave in unexpected
// ways. The following is an example of expressions giving un-intuitive results (i.e.
// results that do not match the behavior of the `=` operator, which is supposed to apply,
// pair-wise, to the comparisons in a CASE or IN statement):
//
//	SELECT -1 IN (0xFF, 18446744073709551615) => 1
//	SELECT -1 IN (0, 18446744073709551615) => 0
//	SELECT -1 IN (0.0, 18446744073709551615) => 1
//
//	SELECT 'FOO' IN ('foo', 0x00) => 0
//	SELECT 'FOO' IN ('foo', 0) => 1
//	SELECT 'FOO' IN ('foo', 0x00, CAST('bar' as char)) => 1
//	SELECT 'FOO' IN ('foo', 0x00, 'bar') => 0
//
//	SELECT 9223372036854775808 IN (0.0e0, -9223372036854775808) => 1
//	SELECT 9223372036854775808 IN (0, -9223372036854775808) => 0
//	SELECT 9223372036854775808 IN (0.0, -9223372036854775808) => 1
//
// Generally speaking, it's counter-intuitive that adding more (unrelated) types to the
// right-hand of the IN operator would change the result of the operation itself. It seems
// like there's logic that changes the way the elements are compared with a type aggregation
// but this is not documented anywhere.
func (bugs) CanCompare(elems ...string) bool {
	var invalid = map[string]string{
		"18446744073709551615": "-1",
		`9223372036854775808`:  `-9223372036854775808`,
	}

	for i, e := range elems {
		if strings.HasPrefix(e, "_binary ") ||
			strings.HasPrefix(e, "0x") ||
			strings.HasPrefix(e, "X'") ||
			strings.HasSuffix(e, "collate utf8mb4_0900_as_cs") {
			return false
		}
		if other, ok := invalid[e]; ok {
			for j := 0; j < len(elems); j++ {
				if i != j && elems[j] == other {
					return false
				}
			}
		}
	}
	return true
}

type Comparison struct {
	Decimals  uint32
	LooseTime bool
}

func (cmp *Comparison) closeDatetime(a, b time.Time, diff time.Duration) bool {
	d := a.Sub(b)
	if d < 0 {
		d = -d
	}
	return d <= diff
}

func (cmp *Comparison) closeFloat(a, b float64) bool {
	const tolerance = 1e-14

	if cmp.Decimals > 0 {
		ratio := math.Pow(10, float64(cmp.Decimals))
		a = math.Round(a*ratio) / ratio
		b = math.Round(b*ratio) / ratio
	}
	if a == b {
		return true
	}
	if b == 0 {
		return math.Abs(a) < tolerance
	}
	return math.Abs((a-b)/b) < tolerance
}

func (cmp *Comparison) Equals(local, remote sqltypes.Value, now time.Time) (bool, error) {
	switch {
	case local.IsFloat() && remote.IsFloat():
		localFloat, err := local.ToFloat64()
		if err != nil {
			return false, fmt.Errorf("error converting local value to float: %w", err)
		}
		remoteFloat, err := remote.ToFloat64()
		if err != nil {
			return false, fmt.Errorf("error converting remote value to float: %w", err)
		}
		return cmp.closeFloat(localFloat, remoteFloat), nil
	case cmp.LooseTime && local.IsDateTime() && remote.IsDateTime():
		localDatetime, _, ok := datetime.ParseDateTime(local.ToString(), -1)
		if !ok {
			return false, fmt.Errorf("error converting local value '%s' to datetime", local)
		}
		remoteDatetime, _, ok := datetime.ParseDateTime(remote.ToString(), -1)
		if !ok {
			return false, fmt.Errorf("error converting remote value '%s' to datetime", remote)
		}
		return cmp.closeDatetime(localDatetime.ToStdTime(now), remoteDatetime.ToStdTime(now), 1*time.Second), nil
	case cmp.LooseTime && local.IsTime() && remote.IsTime():
		localTime, _, state := datetime.ParseTime(local.ToString(), -1)
		if state != datetime.TimeOK {
			return false, fmt.Errorf("error converting local value '%s' to time", local)
		}
		remoteTime, _, state := datetime.ParseTime(remote.ToString(), -1)
		if state != datetime.TimeOK {
			return false, fmt.Errorf("error converting remote value '%s' to time", remote)
		}
		return cmp.closeDatetime(localTime.ToStdTime(now), remoteTime.ToStdTime(now), 1*time.Second), nil
	default:
		return local.String() == remote.String(), nil
	}
}
