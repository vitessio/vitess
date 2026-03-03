/*
Copyright 2019 The Vitess Authors.

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

package mysql

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/sqlerror"
)

func TestIsConnErr(t *testing.T) {
	testcases := []struct {
		in   error
		want bool
	}{{
		in:   errors.New("t"),
		want: false,
	}, {
		in:   sqlerror.NewSQLError(5, "", ""),
		want: false,
	}, {
		in:   sqlerror.NewSQLError(sqlerror.CRServerGone, "", ""),
		want: true,
	}, {
		in:   sqlerror.NewSQLError(sqlerror.CRServerLost, "", ""),
		want: true,
	}, {
		in:   sqlerror.NewSQLError(sqlerror.ERQueryInterrupted, "", ""),
		want: true,
	}, {
		in:   sqlerror.NewSQLError(sqlerror.CRCantReadCharset, "", ""),
		want: false,
	}}
	for _, tcase := range testcases {
		got := sqlerror.IsConnErr(tcase.in)
		assert.Equal(t, tcase.want, got, "IsConnErr(%#v): %v, want %v", tcase.in, got, tcase.want)
	}
}

func TestIsConnLostDuringQuery(t *testing.T) {
	testcases := []struct {
		in   error
		want bool
	}{{
		in:   errors.New("t"),
		want: false,
	}, {
		in:   sqlerror.NewSQLError(5, "", ""),
		want: false,
	}, {
		in:   sqlerror.NewSQLError(sqlerror.CRServerGone, "", ""),
		want: false,
	}, {
		in:   sqlerror.NewSQLError(sqlerror.CRServerLost, "", ""),
		want: true,
	}, {
		in:   sqlerror.NewSQLError(sqlerror.ERQueryInterrupted, "", ""),
		want: false,
	}, {
		in:   sqlerror.NewSQLError(sqlerror.CRCantReadCharset, "", ""),
		want: false,
	}}
	for _, tcase := range testcases {
		got := sqlerror.IsConnLostDuringQuery(tcase.in)
		assert.Equal(t, tcase.want, got, "IsConnLostDuringQuery(%#v): %v, want %v", tcase.in, got, tcase.want)
	}
}

func TestClampZstdLevel(t *testing.T) {
	testcases := []struct {
		name string
		in   int
		want int
	}{
		{name: "below min uses default", in: 0, want: zstdCompressionLevelDefault},
		{name: "negative uses default", in: -5, want: zstdCompressionLevelDefault},
		{name: "min boundary", in: 1, want: 1},
		{name: "mid range", in: 10, want: 10},
		{name: "max boundary", in: 22, want: 22},
		{name: "above max clamped", in: 23, want: zstdCompressionLevelMax},
		{name: "far above max clamped", in: 100, want: zstdCompressionLevelMax},
		{name: "default value", in: 3, want: 3},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			got := clampZstdLevel(tc.in)
			assert.Equal(t, tc.want, got)
		})
	}
}
