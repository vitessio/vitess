/*
Copyright 2021 The Vitess Authors.

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

package uca

type Reorder struct {
	FromMin, FromMax uint16
	ToMin, ToMax     uint16
}

type parametricT struct {
	upperCaseFirst bool
	reorder        []Reorder
	reorderMin     uint16
	reorderMax     uint16
}

func (p *parametricT) adjust(level int, weight uint16) uint16 {
	if p == nil {
		return weight
	}
	switch level {
	case 0:
		if weight >= p.reorderMin && weight <= p.reorderMax {
			for _, reorder := range p.reorder {
				if weight >= reorder.FromMin && weight <= reorder.FromMax {
					return weight - reorder.FromMin + reorder.ToMin
				}
			}
		}
	case 2:
		// see: https://unicode.org/reports/tr35/tr35-collation.html#Case_Untailored
		if p.upperCaseFirst && weight < ' ' {
			switch weight {
			case 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0E, 0x11, 0x12, 0x1D:
				return weight | 0x100
			default:
				return weight | 0x300
			}
		}
	}
	return weight
}

func newParametricTailoring(reorder []Reorder, upperCaseFirst bool) *parametricT {
	if len(reorder) == 0 && !upperCaseFirst {
		return nil
	}

	t := &parametricT{
		upperCaseFirst: upperCaseFirst,
		reorder:        reorder,
		reorderMin:     ^uint16(0),
		reorderMax:     0,
	}

	for _, r := range reorder {
		if r.FromMin < t.reorderMin {
			t.reorderMin = r.FromMin
		}
		if r.FromMax > t.reorderMax {
			t.reorderMax = r.FromMax
		}
	}

	return t
}
