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

package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vschemawrapper"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestDeniedSystemVariables(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(t, "vschemas/schema.json", true)

	cases := []struct {
		name     string
		denied   map[string]struct{}
		query    string
		wantErr  string // empty = expect success
		wantCode vtrpcpb.Code
	}{{
		name:     "unique_checks denied returns unsupported",
		denied:   map[string]struct{}{"unique_checks": {}},
		query:    "set unique_checks = 0",
		wantErr:  "VT12001: unsupported: system setting: unique_checks",
		wantCode: vtrpcpb.Code_UNIMPLEMENTED,
	}, {
		name:   "unique_checks not denied succeeds",
		denied: nil,
		query:  "set unique_checks = 0",
	}, {
		name:     "case-insensitive match",
		denied:   map[string]struct{}{"unique_checks": {}},
		query:    "set UNIQUE_CHECKS = 0",
		wantErr:  "VT12001: unsupported: system setting: UNIQUE_CHECKS",
		wantCode: vtrpcpb.Code_UNIMPLEMENTED,
	}, {
		name:     "denylist applies to VitessAware sysvars",
		denied:   map[string]struct{}{"autocommit": {}},
		query:    "set autocommit = 0",
		wantErr:  "VT12001: unsupported: system setting: autocommit",
		wantCode: vtrpcpb.Code_UNIMPLEMENTED,
	}, {
		name:   "unrelated sysvars are unaffected",
		denied: map[string]struct{}{"unique_checks": {}},
		query:  "set foreign_key_checks = 0",
	}, {
		// An unknown sysvar should still return the existing VT05006
		// "unknown system variable" error, not the denylist error.
		name:     "unknown sysvar still returns unknown-variable error",
		denied:   map[string]struct{}{"unique_checks": {}},
		query:    "set not_a_real_sysvar = 1",
		wantErr:  "VT05006: unknown system variable '@@not_a_real_sysvar = 1'",
		wantCode: vtrpcpb.Code_NOT_FOUND,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
			require.NoError(t, err)
			vw.DeniedSysVars = tc.denied

			_, err = TestBuilder(tc.query, vw, "")
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, tc.wantErr, err.Error())
			assert.Equal(t, tc.wantCode, vterrors.Code(err))
		})
	}
}
