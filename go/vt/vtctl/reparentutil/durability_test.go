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

package reparentutil

import (
	"testing"

	"vitess.io/vitess/go/vt/topo/topoproto"

	"github.com/stretchr/testify/assert"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestDurabilitySpecified(t *testing.T) {
	cellName := "cell"
	durabilityRules := newDurabilitySpecified(
		map[string]string{
			"cell-0000000000": string(MustPromoteRule),
			"cell-0000000001": string(PreferPromoteRule),
			"cell-0000000002": string(NeutralPromoteRule),
			"cell-0000000003": string(PreferNotPromoteRule),
			"cell-0000000004": string(MustNotPromoteRule),
		})

	testcases := []struct {
		tablet        *topodatapb.Tablet
		promotionRule CandidatePromotionRule
	}{
		{
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: cellName,
					Uid:  0,
				},
			},
			promotionRule: MustNotPromoteRule,
		}, {
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: cellName,
					Uid:  1,
				},
			},
			promotionRule: PreferPromoteRule,
		}, {
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: cellName,
					Uid:  2,
				},
			},
			promotionRule: NeutralPromoteRule,
		}, {
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: cellName,
					Uid:  3,
				},
			},
			promotionRule: PreferNotPromoteRule,
		}, {
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: cellName,
					Uid:  4,
				},
			},
			promotionRule: MustNotPromoteRule,
		},
	}

	for _, testcase := range testcases {
		t.Run(topoproto.TabletAliasString(testcase.tablet.Alias), func(t *testing.T) {
			rule := durabilityRules.promotionRule(testcase.tablet)
			assert.Equal(t, testcase.promotionRule, rule)
		})
	}
}
