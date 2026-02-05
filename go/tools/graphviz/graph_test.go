/*
Copyright 2022 The Vitess Authors.

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

package graphviz

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimple(t *testing.T) {
	g := New()
	n1 := g.AddNode("apa")
	n1.AddAttribute("aåö**|")
	n1.AddAttribute("value")
	n2 := g.AddNode("banan")
	g.AddEdge(n1, n2)

	fmt.Println(g.produceDot())

	err := g.Render()
	require.NoError(t, err)
}
