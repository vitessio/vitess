/*
Copyright 2020 The Vitess Authors.

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

	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type fakePlan struct {
	solve semantics.TableSet
}

func (f *fakePlan) solves() semantics.TableSet {
	return f.solve
}

func (f *fakePlan) cost() int {
	return 1
}

var _ joinTree = (*fakePlan)(nil)

func TestDpTableSizeOf(t *testing.T) {
	dpTable := makeDPTable()

	a := semantics.TableSet(1)
	b := semantics.TableSet(2)

	t1 := &fakePlan{solve: a}
	t2 := &fakePlan{solve: b}
	t3 := &fakePlan{solve: a.Merge(b)}

	dpTable.add(t1)
	dpTable.add(t2)
	dpTable.add(t3)

	size1 := dpTable.bitSetsOfSize(1)
	assert.Equal(t, []joinTree{t1, t2}, size1, "size 1")

	size2 := dpTable.bitSetsOfSize(2)
	assert.Equal(t, []joinTree{t3}, size2, "size 2")
	assert.Equal(t, t1, dpTable.planFor(a))
	assert.Equal(t, t2, dpTable.planFor(b))
	assert.Equal(t, t3, dpTable.planFor(a.Merge(b)))
}
