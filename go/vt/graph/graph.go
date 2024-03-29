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
	"fmt"
	"maps"
	"slices"
	"strings"
)

const (
	white int = iota
	grey
	black
)

// Graph is a generic graph implementation.
type Graph[C comparable] struct {
	edges           map[C][]C
	orderedVertices []C
}

// NewGraph creates a new graph for the given comparable type.
func NewGraph[C comparable]() *Graph[C] {
	return &Graph[C]{
		edges:           map[C][]C{},
		orderedVertices: []C{},
	}
}

// AddVertex adds a vertex to the given Graph.
func (gr *Graph[C]) AddVertex(vertex C) {
	_, alreadyExists := gr.edges[vertex]
	if alreadyExists {
		return
	}
	gr.edges[vertex] = []C{}
	gr.orderedVertices = append(gr.orderedVertices, vertex)
}

// AddEdge adds an edge to the given Graph.
// It also makes sure that the vertices are added to the graph if not already present.
func (gr *Graph[C]) AddEdge(start C, end C) {
	gr.AddVertex(start)
	gr.AddVertex(end)
	gr.edges[start] = append(gr.edges[start], end)
}

// PrintGraph is used to print the graph. This is only used for testing.
func (gr *Graph[C]) PrintGraph() string {
	adjacencyLists := []string{}
	for vertex, edges := range gr.edges {
		adjacencyList := fmt.Sprintf("%v -", vertex)
		for _, end := range edges {
			adjacencyList += fmt.Sprintf(" %v", end)
		}
		adjacencyLists = append(adjacencyLists, adjacencyList)
	}
	slices.Sort(adjacencyLists)
	return strings.Join(adjacencyLists, "\n")
}

// Empty checks whether the graph is empty or not.
func (gr *Graph[C]) Empty() bool {
	return len(gr.edges) == 0
}

// HasCycles checks whether the given graph has a cycle or not.
// We are using a well-known DFS based colouring algorithm to check for cycles.
// Look at https://cp-algorithms.com/graph/finding-cycle.html for more details on the algorithm.
func (gr *Graph[C]) HasCycles() (bool, []C) {
	// If the graph is empty, then we don't need to check anything.
	if gr.Empty() {
		return false, nil
	}
	// Initialize the coloring map.
	// 0 represents white.
	// 1 represents grey.
	// 2 represents black.
	color := map[C]int{}
	for vertex := range gr.edges {
		// If any vertex is still white, we initiate a new DFS.
		if color[vertex] == white {
			if hasCycle, cycle := gr.hasCyclesDfs(color, vertex); hasCycle {
				return true, cycle
			}
		}
	}
	return false, nil
}

// GetCycles returns all known cycles in the graph.
// It returns a map of vertices to the cycle they are part of.
// We are using a well-known DFS based colouring algorithm to check for cycles.
// Look at https://cp-algorithms.com/graph/finding-cycle.html for more details on the algorithm.
func (gr *Graph[C]) GetCycles() (vertices map[C][]C) {
	// If the graph is empty, then we don't need to check anything.
	if gr.Empty() {
		return nil
	}
	vertices = make(map[C][]C)
	// Initialize the coloring map.
	// 0 represents white.
	// 1 represents grey.
	// 2 represents black.
	color := map[C]int{}
	for _, vertex := range gr.orderedVertices {
		// If any vertex is still white, we initiate a new DFS.
		if color[vertex] == white {
			// We clone the colors because we wnt full coverage for all vertices.
			// Otherwise, the algorithm is optimal and stop more-or-less after the first cycle.
			color := maps.Clone(color)
			if hasCycle, cycle := gr.hasCyclesDfs(color, vertex); hasCycle {
				vertices[vertex] = cycle
			}
		}
	}
	return vertices
}

// hasCyclesDfs is a utility function for checking for cycles in a graph.
// It runs a dfs from the given vertex marking each vertex as grey. During the dfs,
// if we encounter a grey vertex, we know we have a cycle. We mark the visited vertices black
// on finishing the dfs.
func (gr *Graph[C]) hasCyclesDfs(color map[C]int, vertex C) (bool, []C) {
	// Mark the vertex grey.
	color[vertex] = grey
	result := []C{vertex}
	// Go over all the edges.
	for _, end := range gr.edges[vertex] {
		// If we encounter a white vertex, we continue the dfs.
		if color[end] == white {
			if hasCycle, cycle := gr.hasCyclesDfs(color, end); hasCycle {
				return true, append(result, cycle...)
			}
		} else if color[end] == grey {
			// We encountered a grey vertex, we have a cycle.
			return true, append(result, end)
		}
	}
	// Mark the vertex black before finishing
	color[vertex] = black
	return false, nil
}
