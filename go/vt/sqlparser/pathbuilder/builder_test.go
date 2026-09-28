/*
Copyright 2025 The Vitess Authors.

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

package pathbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestASTPathBuilderAddStep(t *testing.T) {
	tests := []struct {
		name     string
		steps    []uint16
		wantPath string
	}{
		{
			name:     "single step",
			steps:    []uint16{1},
			wantPath: "\x00\x01",
		},
		{
			name:     "multiple steps",
			steps:    []uint16{1, 0x24, 0x913},
			wantPath: "\x00\x01\x00\x24\x09\x13",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apb := NewASTPathBuilder()
			for _, step := range tt.steps {
				apb.AddStep(step)
			}
			require.Equal(t, tt.wantPath, apb.ToPath())
		})
	}
}

func TestASTPathBuilderAddStepOffset(t *testing.T) {
	tests := []struct {
		name     string
		steps    []uint16
		offsets  []int
		wantPath string
	}{
		{
			name:     "single step",
			steps:    []uint16{1},
			offsets:  []int{0},
			wantPath: "\x00\x01\x00",
		},
		{
			name:     "multiple steps",
			steps:    []uint16{1, 0x24, 0x913, 0x913},
			offsets:  []int{0, -1, 0, 1}, // the 0 is overwritten by the last offset
			wantPath: "\x00\x01\x00\x00\x24\x09\x13\x01",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apb := NewASTPathBuilder()
			for idx, step := range tt.steps {
				switch tt.offsets[idx] {
				case -1:
					apb.AddStep(step)
				case 0:
					apb.AddStepWithOffset(step)
				default:
					apb.ChangeOffset(tt.offsets[idx])
				}
			}
			require.Equal(t, tt.wantPath, apb.ToPath())
		})
	}
}

func TestASTPathBuilderPop(t *testing.T) {
	tests := []struct {
		name     string
		steps    []uint16
		offsets  []int
		wantPath string
	}{
		{
			name:     "single step",
			steps:    []uint16{1},
			offsets:  []int{0},
			wantPath: "",
		},
		{
			name:     "multiple steps - final step with offset",
			steps:    []uint16{1, 0x24, 0x913},
			offsets:  []int{0, -1, 0, 1},
			wantPath: "\x00\x01\x00\x00\x24",
		},
		{
			name:     "multiple steps - final step without offset",
			steps:    []uint16{1, 0x24, 0x913},
			offsets:  []int{0, -1, -1},
			wantPath: "\x00\x01\x00\x00\x24",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apb := NewASTPathBuilder()
			for idx, step := range tt.steps {
				switch tt.offsets[idx] {
				case -1:
					apb.AddStep(step)
				case 0:
					apb.AddStepWithOffset(step)
				default:
					apb.ChangeOffset(tt.offsets[idx])
				}
			}
			apb.Pop()
			require.Equal(t, tt.wantPath, apb.ToPath())
		})
	}
}
