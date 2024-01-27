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

package sqltypes

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryResponsesEqual(t *testing.T) {
	tests := []struct {
		name    string
		r1      []QueryResponse
		r2      []QueryResponse
		isEqual bool
	}{
		{
			name: "1 response in each",
			r1: []QueryResponse{
				{
					QueryResult: &Result{},
					QueryError:  nil,
				},
			},
			r2: []QueryResponse{
				{
					QueryResult: &Result{},
					QueryError:  nil,
				},
			},
			isEqual: true,
		},
		{
			name: "different lengths",
			r1: []QueryResponse{
				{
					QueryResult: &Result{},
					QueryError:  nil,
				},
			},
			r2:      []QueryResponse{},
			isEqual: false,
		},
		{
			name: "different query errors",
			r1: []QueryResponse{
				{
					QueryResult: &Result{},
					QueryError:  fmt.Errorf("some error"),
				},
			},
			r2: []QueryResponse{
				{
					QueryResult: &Result{
						Info: "Test",
					},
					QueryError: nil,
				},
			},
			isEqual: false,
		},
		{
			name: "different query results",
			r1: []QueryResponse{
				{
					QueryResult: &Result{
						RowsAffected: 7,
					},
					QueryError: nil,
				},
			},
			r2: []QueryResponse{
				{
					QueryResult: &Result{
						RowsAffected: 10,
					},
					QueryError: nil,
				},
			},
			isEqual: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.isEqual, QueryResponsesEqual(tt.r1, tt.r2))
		})
	}
}
