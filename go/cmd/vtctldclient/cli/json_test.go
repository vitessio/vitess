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

package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConvertToSnakeCase(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want any
	}{
		{
			name: "string",
			val:  "MyValIsNotCool",
			want: "my_val_is_not_cool",
		},
		{
			name: "map[string]bool",
			val:  map[string]any{"MyValIsNotCool": true},
			want: map[string]any{"my_val_is_not_cool": true},
		},
		{
			name: "[]string",
			val: []string{
				"MyValIsNotCool",
				"NeitherIsYours",
			},
			want: []string{
				"my_val_is_not_cool",
				"neither_is_yours",
			},
		},
		{
			name: "map[string][any]",
			val: map[string]any{
				"MyValIsNotCool": "val1",
				"NeitherIsYours": "val2",
			},
			want: map[string]any{
				"my_val_is_not_cool": "val1",
				"neither_is_yours":   "val2",
			},
		},
		{
			name: "map[any]any",
			val: map[any]any{
				"MyValIsNotCool": []any{
					0: map[any]any{
						"SubKey1": "val1",
						"SubKey2": "val2",
					},
					1: map[any]any{
						"SubKey3": "val3",
						"SubKey4": "val4",
					},
				},
				"NeitherIsYours": []any{
					0: map[any]any{
						"SubKey5": "val5",
						"SubKey6": "val6",
					},
					1: map[any]any{
						"SubKey7": "val7",
						"SubKey8": "val8",
					},
				},
			},
			want: map[any]any{
				"my_val_is_not_cool": []any{
					0: map[any]any{
						"sub_key1": "val1",
						"sub_key2": "val2",
					},
					1: map[any]any{
						"sub_key3": "val3",
						"sub_key4": "val4",
					},
				},
				"neither_is_yours": []any{
					0: map[any]any{
						"sub_key5": "val5",
						"sub_key6": "val6",
					},
					1: map[any]any{
						"sub_key7": "val7",
						"sub_key8": "val8",
					},
				},
			},
		},
		{
			name: "map[any][]any",
			val: map[any]any{
				"MyValIsNotCool": []any{"val1", "val2"},
				"NeitherIsYours": []any{"val3", "val4"},
			},
			want: map[any]any{
				"my_val_is_not_cool": []any{"val1", "val2"},
				"neither_is_yours":   []any{"val3", "val4"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertToSnakeCase(tt.val)
			require.NoError(t, err, "ConvertToSnakeCase() error = %v", err)
			require.EqualValues(t, tt.want, got, "ConvertToSnakeCase() = %v, want %v", got, tt.want)
		})
	}
}
