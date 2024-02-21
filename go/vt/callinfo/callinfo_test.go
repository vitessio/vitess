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

package callinfo

import (
	"context"
	"testing"

	"github.com/google/safehtml"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/callinfo/fakecallinfo"
)

var fci fakecallinfo.FakeCallInfo = fakecallinfo.FakeCallInfo{
	User:   "test",
	Remote: "locahost",
	Method: "",
	Html:   safehtml.HTML{},
}

func TestNewContext(t *testing.T) {
	tests := []struct {
		name            string
		ctx             context.Context
		ci              CallInfo
		expectedContext context.Context
	}{
		{
			name:            "empty",
			ctx:             context.Background(),
			ci:              nil,
			expectedContext: context.WithValue(context.Background(), callInfoKey, nil),
		},
		{
			name:            "not empty",
			ctx:             context.Background(),
			ci:              &fci,
			expectedContext: context.WithValue(context.Background(), callInfoKey, &fci),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expectedContext, NewContext(tt.ctx, tt.ci))
		})
	}
}

func TestFromContext(t *testing.T) {
	tests := []struct {
		name       string
		ctx        context.Context
		expectedCi CallInfo
		ok         bool
	}{
		{
			name:       "empty",
			ctx:        context.WithValue(context.Background(), callInfoKey, nil),
			expectedCi: nil,
			ok:         false,
		},
		{
			name:       "not empty",
			expectedCi: &fci,
			ctx:        context.WithValue(context.Background(), callInfoKey, &fci),
			ok:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ci, ok := FromContext(tt.ctx)
			require.Equal(t, tt.expectedCi, ci)
			require.Equal(t, tt.ok, ok)
		})
	}
}

func TestHTMLFromContext(t *testing.T) {
	tests := []struct {
		name         string
		ctx          context.Context
		expectedHTML safehtml.HTML
	}{
		{
			name:         "empty",
			ctx:          context.WithValue(context.Background(), callInfoKey, nil),
			expectedHTML: safehtml.HTML{},
		},
		{
			name:         "not empty",
			ctx:          context.WithValue(context.Background(), callInfoKey, &fci),
			expectedHTML: safehtml.HTML{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expectedHTML, HTMLFromContext(tt.ctx))
		})
	}
}
