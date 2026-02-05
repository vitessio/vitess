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
