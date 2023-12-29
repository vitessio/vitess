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

package planbuilder

import (
	"vitess.io/vitess/go/vt/vtgate/engine"
)

type (
	mirror struct {
		plan    logicalPlan
		targets []mirrorTarget
	}

	mirrorTarget interface {
		Primitive() engine.MirrorTarget
	}

	percentMirrorTarget struct {
		percent float32
		plan    logicalPlan
	}
)

var (
	_ logicalPlan  = (*mirror)(nil)
	_ mirrorTarget = (*percentMirrorTarget)(nil)
)

// Primitive implements the logicalPlan interface
func (c *mirror) Primitive() engine.Primitive {
	mirrorTargets := make([]engine.MirrorTarget, len(c.targets))

	for i, target := range c.targets {
		mirrorTargets[i] = target.Primitive()
	}

	return &engine.Mirror{
		Primitive: c.plan.Primitive(),
		Targets:   mirrorTargets,
	}
}

func (c *percentMirrorTarget) Primitive() engine.MirrorTarget {
	return &engine.PercentMirrorTarget{
		Percent: c.percent,
		Primitive: &engine.NonCommittal{
			Primitive: c.plan.Primitive(),
		},
	}
}
