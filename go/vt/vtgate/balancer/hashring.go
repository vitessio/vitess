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

package balancer

import (
	"slices"
	"sort"
	"strconv"
	"sync"

	"github.com/cespare/xxhash/v2"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// defaultVirtualNodes is the default number of virtual nodes to use in
// the hash ring.
const defaultVirtualNodes = 16

// hashRing represents a hash ring of tablets.
type hashRing struct {
	mu sync.RWMutex

	// nodes is the sorted list of virtual nodes.
	nodes []uint64

	// numVirtualNodes is the number of virtual nodes each member of
	// the hash ring has.
	numVirtualNodes int

	// nodeMap is a map from a tablet's hash to the tablet.
	nodeMap map[uint64]*discovery.TabletHealth

	// tablets is a "set" of all the tablets currently in the hash ring (by alias).
	tablets map[string]struct{}
}

// newHashRing returns a new hash ring with the default number of virtual nodes.
func newHashRing() *hashRing {
	return &hashRing{
		numVirtualNodes: defaultVirtualNodes,
		nodeMap:         make(map[uint64]*discovery.TabletHealth),
		tablets:         make(map[string]struct{}),
	}
}

// add adds a tablet to the hash ring.
func (r *hashRing) add(tablet *discovery.TabletHealth) {
	if r.contains(tablet) {
		return
	}

	// Build the tablet's hashes before locking
	hashes := make([]uint64, 0, r.numVirtualNodes)
	for i := range r.numVirtualNodes {
		hash := buildHash(tablet, i)
		hashes = append(hashes, hash)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, hash := range hashes {
		r.nodes = append(r.nodes, hash)
		r.nodeMap[hash] = tablet
	}

	slices.Sort(r.nodes)
	r.tablets[tabletAlias(tablet)] = struct{}{}
}

// remove removes a tablet from the hash ring.
func (r *hashRing) remove(tablet *discovery.TabletHealth) {
	if !r.contains(tablet) {
		return
	}

	// Build the tablet's hashes before locking
	hashes := make(map[uint64]struct{}, r.numVirtualNodes)
	for i := range r.numVirtualNodes {
		hash := buildHash(tablet, i)
		hashes[hash] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for hash := range hashes {
		delete(r.nodeMap, hash)
	}

	r.nodes = removeNodes(r.nodes, hashes)
	delete(r.tablets, tabletAlias(tablet))
}

// get returns the tablet for the given key, ignoring invalid tablets.
func (r *hashRing) get(key string, invalidTablets map[string]bool) *discovery.TabletHealth {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return nil
	}

	hash := xxhash.Sum64String(key)

	// Find the first node greater than or equal to this hash, and isn't invalid
	i := sort.Search(len(r.nodes), func(i int) bool {
		node := r.nodes[i]

		return !r.invalidNode(node, invalidTablets) && r.nodes[i] >= hash
	})

	// Wrap around if needed
	if i == len(r.nodes) {
		i = 0

		// If the first tablet is invalid, it means we couldn't find any valid tablets
		node := r.nodes[i]
		if r.invalidNode(node, invalidTablets) {
			return nil
		}
	}

	// Return the associated tablet
	node := r.nodes[i]
	tablet := r.nodeMap[node]

	return tablet
}

// contains checks if a tablet exists in the hash ring.
func (r *hashRing) contains(tablet *discovery.TabletHealth) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.tablets[tabletAlias(tablet)]
	return exists
}

// sort sorts the list of nodes.
func (r *hashRing) sort() {
	r.mu.Lock()
	defer r.mu.Unlock()

	slices.Sort(r.nodes)
}

// buildHash builds a virtual node hash.
func buildHash(tablet *discovery.TabletHealth, node int) uint64 {
	key := tabletAlias(tablet) + "#" + strconv.Itoa(node)
	hash := xxhash.Sum64String(key)

	return hash
}

// tabletAlias returns the tablet's alias as a string.
func tabletAlias(tablet *discovery.TabletHealth) string {
	return topoproto.TabletAliasString(tablet.Tablet.Alias)
}

// removeNodes removes the nodes in the set of hashes from the given list of nodes.
func removeNodes(nodes []uint64, hashes map[uint64]struct{}) []uint64 {
	// Update the node list in-place

	writeIdx := 0
	for _, node := range nodes {
		// Check if this node belongs to the tablet being removed
		_, isTabletNode := hashes[node]
		if isTabletNode {
			continue
		}

		nodes[writeIdx] = node
		writeIdx++
	}

	return nodes[:writeIdx]
}

// invalidNode returns whether the virtual node is associated with an invalid tablet.
func (r *hashRing) invalidNode(node uint64, invalidTablets map[string]bool) bool {
	tablet := r.nodeMap[node]

	alias := topoproto.TabletAliasString(tablet.Tablet.Alias)
	_, invalid := invalidTablets[alias]

	return invalid
}
