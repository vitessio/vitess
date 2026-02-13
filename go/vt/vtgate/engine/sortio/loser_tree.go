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

package sortio

import "vitess.io/vitess/go/sqltypes"

// loserTree implements a tournament tree (loser tree) for K-way merging.
// Internal nodes store the loser of each match; the overall winner is
// stored at nodes[0]. Replacing the winner and replaying costs exactly
// ceil(log2(K)) comparisons — half the cost of a binary heap's Pop+Push.
//
// Tree layout: K leaves are conceptually at positions [K, 2K).
// Internal nodes are at positions [1, K). nodes[0] stores the winner.
// Parent of position p is p/2. Children of node p are 2p and 2p+1.
// Leaf index i maps to position i+K, so its parent node is (i+K)/2.
type loserTree struct {
	// nodes has length K. nodes[0] holds the overall winner index.
	// nodes[1..K-1] store the loser leaf index at each internal node.
	nodes []int
	// leaves[0..K-1] hold the current row from each source.
	leaves []mergeEntry
	// count is the number of active (non-exhausted) sources.
	count int
	// less returns true if a < b.
	less func(a, b sqltypes.Row) bool
}

type mergeEntry struct {
	row    sqltypes.Row
	source int
	dead   bool
}

// newLoserTree builds a loser tree from the given entries.
func newLoserTree(entries []mergeEntry, less func(a, b sqltypes.Row) bool) *loserTree {
	k := len(entries)
	if k == 0 {
		return &loserTree{less: less}
	}

	t := &loserTree{
		nodes:  make([]int, k),
		leaves: entries,
		count:  k,
		less:   less,
	}

	if k == 1 {
		t.nodes[0] = 0
		return t
	}

	// Build the tree with recursive pairwise tournaments.
	// Each internal node compares two subtree winners, stores the loser,
	// and returns the winner up.
	t.nodes[0] = t.build(1)
	return t
}

// build recursively determines the winner from the subtree rooted at
// the given internal node. The loser is stored at nodes[node].
func (t *loserTree) build(node int) int {
	k := len(t.leaves)
	left := 2 * node
	right := 2*node + 1

	var leftWinner, rightWinner int
	if left >= k {
		leftWinner = left - k
	} else {
		leftWinner = t.build(left)
	}
	if right >= k {
		rightWinner = right - k
	} else {
		rightWinner = t.build(right)
	}

	if t.beats(leftWinner, rightWinner) {
		t.nodes[node] = rightWinner
		return leftWinner
	}
	t.nodes[node] = leftWinner
	return rightWinner
}

// beats returns true if leaf a should be preferred over leaf b.
// Dead leaves always lose (are never preferred).
func (t *loserTree) beats(a, b int) bool {
	if t.leaves[b].dead {
		return true
	}
	if t.leaves[a].dead {
		return false
	}
	return t.less(t.leaves[a].row, t.leaves[b].row)
}

// Len returns the number of active (non-exhausted) sources.
func (t *loserTree) Len() int {
	return t.count
}

// Winner returns the current minimum row and its source index.
func (t *loserTree) Winner() (sqltypes.Row, int) {
	w := t.nodes[0]
	return t.leaves[w].row, t.leaves[w].source
}

// Replace replaces the winner's row with a new row from the same source
// and replays from the winner's leaf to the root. O(log K) comparisons.
func (t *loserTree) Replace(row sqltypes.Row) {
	w := t.nodes[0]
	t.leaves[w].row = row
	t.replay(w)
}

// Remove marks the winner's source as exhausted and replays.
// The dead leaf will always lose future comparisons.
func (t *loserTree) Remove() {
	w := t.nodes[0]
	t.leaves[w].dead = true
	t.leaves[w].row = nil
	t.count--
	if t.count > 0 {
		t.replay(w)
	}
}

// replay walks from leaf idx up to the root, updating losers.
// At each internal node, the incoming winner is compared with the stored
// loser. The loser stays at the node; the winner continues up.
func (t *loserTree) replay(idx int) {
	k := len(t.leaves)
	winner := idx

	for pos := (idx + k) / 2; pos > 0; pos /= 2 {
		stored := t.nodes[pos]
		if t.beats(stored, winner) {
			t.nodes[pos] = winner
			winner = stored
		}
	}

	t.nodes[0] = winner
}
