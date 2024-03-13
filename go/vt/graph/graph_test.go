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

package graph

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIntegerGraph tests that a graph with Integers can be created and all graph functions work as intended.
func TestIntegerGraph(t *testing.T) {
	testcases := []struct {
		name          string
		edges         [][2]int
		wantedGraph   string
		wantEmpty     bool
		wantHasCycles bool
	}{
		{
			name:          "empty graph",
			edges:         nil,
			wantedGraph:   "",
			wantEmpty:     true,
			wantHasCycles: false,
		}, {
			name: "non-cyclic graph",
			edges: [][2]int{
				{1, 2},
				{2, 3},
				{1, 4},
				{2, 5},
				{4, 5},
			},
			wantedGraph: `1 - 2 4
2 - 3 5
3 -
4 - 5
5 -`,
			wantEmpty:     false,
			wantHasCycles: false,
		}, {
			name: "cyclic graph",
			edges: [][2]int{
				{1, 2},
				{2, 3},
				{1, 4},
				{2, 5},
				{4, 5},
				{5, 6},
				{6, 1},
			},
			wantedGraph: `1 - 2 4
2 - 3 5
3 -
4 - 5
5 - 6
6 - 1`,
			wantEmpty:     false,
			wantHasCycles: true,
		},
	}
	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			graph := NewGraph[int]()
			for _, edge := range tt.edges {
				graph.AddEdge(edge[0], edge[1])
			}
			require.Equal(t, tt.wantedGraph, graph.PrintGraph())
			require.Equal(t, tt.wantEmpty, graph.Empty())
			hasCycle, _ := graph.HasCycles()
			require.Equal(t, tt.wantHasCycles, hasCycle)
		})
	}
}

// TestStringGraph tests that a graph with strings can be created and all graph functions work as intended.
func TestStringGraph(t *testing.T) {
	testcases := []struct {
		name          string
		edges         [][2]string
		wantedGraph   string
		wantEmpty     bool
		wantHasCycles bool
		wantCycles    map[string][]string
	}{
		{
			name:          "empty graph",
			edges:         nil,
			wantedGraph:   "",
			wantEmpty:     true,
			wantHasCycles: false,
		}, {
			name: "non-cyclic graph",
			edges: [][2]string{
				{"A", "B"},
				{"B", "C"},
				{"A", "D"},
				{"B", "E"},
				{"D", "E"},
			},
			wantedGraph: `A - B D
B - C E
C -
D - E
E -`,
			wantEmpty:     false,
			wantHasCycles: false,
		}, {
			name: "cyclic graph",
			edges: [][2]string{
				{"A", "B"},
				{"B", "C"},
				{"A", "D"},
				{"B", "E"},
				{"D", "E"},
				{"E", "F"},
				{"F", "A"},
			},
			wantedGraph: `A - B D
B - C E
C -
D - E
E - F
F - A`,
			wantEmpty:     false,
			wantHasCycles: true,
			wantCycles: map[string][]string{
				"A": {"A", "B", "E", "F", "A"},
				"B": {"B", "E", "F", "A", "B"},
				"D": {"D", "E", "F", "A", "B", "E"},
				"E": {"E", "F", "A", "B", "E"},
				"F": {"F", "A", "B", "E", "F"},
			},
		},
	}
	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			graph := NewGraph[string]()
			for _, edge := range tt.edges {
				graph.AddEdge(edge[0], edge[1])
			}
			require.Equal(t, tt.wantedGraph, graph.PrintGraph())
			require.Equal(t, tt.wantEmpty, graph.Empty())
			hasCycle, _ := graph.HasCycles()
			require.Equal(t, tt.wantHasCycles, hasCycle)
			if tt.wantCycles == nil {
				tt.wantCycles = map[string][]string{}
			}
			actualCycles := graph.GetCycles()
			if actualCycles == nil {
				actualCycles = map[string][]string{}
			}
			require.Equal(t, tt.wantCycles, actualCycles)
		})
	}
}
