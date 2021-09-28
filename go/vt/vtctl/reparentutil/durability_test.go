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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
)

func TestDurabilityNone(t *testing.T) {
	err := SetDurabilityPolicy("none", nil)
	require.NoError(t, err)

	promoteRule := PromotionRule(&topodatapb.Tablet{
		Type: topodatapb.TabletType_PRIMARY,
	})
	assert.Equal(t, promotionrule.Neutral, promoteRule)

	promoteRule = PromotionRule(&topodatapb.Tablet{
		Type: topodatapb.TabletType_REPLICA,
	})
	assert.Equal(t, promotionrule.Neutral, promoteRule)

	promoteRule = PromotionRule(&topodatapb.Tablet{
		Type: topodatapb.TabletType_RDONLY,
	})
	assert.Equal(t, promotionrule.MustNot, promoteRule)

	promoteRule = PromotionRule(&topodatapb.Tablet{
		Type: topodatapb.TabletType_SPARE,
	})
	assert.Equal(t, promotionrule.MustNot, promoteRule)
	assert.Equal(t, 0, PrimarySemiSyncFromTablet(nil))
	assert.Equal(t, false, ReplicaSemiSyncFromTablet(nil, nil))
}

func TestDurabilitySemiSync(t *testing.T) {
	err := SetDurabilityPolicy("semi_sync", nil)
	require.NoError(t, err)

	promoteRule := PromotionRule(&topodatapb.Tablet{
		Type: topodatapb.TabletType_PRIMARY,
	})
	assert.Equal(t, promotionrule.Neutral, promoteRule)

	promoteRule = PromotionRule(&topodatapb.Tablet{
		Type: topodatapb.TabletType_REPLICA,
	})
	assert.Equal(t, promotionrule.Neutral, promoteRule)

	promoteRule = PromotionRule(&topodatapb.Tablet{
		Type: topodatapb.TabletType_RDONLY,
	})
	assert.Equal(t, promotionrule.MustNot, promoteRule)

	promoteRule = PromotionRule(&topodatapb.Tablet{
		Type: topodatapb.TabletType_SPARE,
	})
	assert.Equal(t, promotionrule.MustNot, promoteRule)
	assert.Equal(t, 1, PrimarySemiSyncFromTablet(nil))
	assert.Equal(t, true, ReplicaSemiSyncFromTablet(nil, &topodatapb.Tablet{
		Type: topodatapb.TabletType_REPLICA,
	}))
	assert.Equal(t, false, ReplicaSemiSyncFromTablet(nil, &topodatapb.Tablet{
		Type: topodatapb.TabletType_EXPERIMENTAL,
	}))
}

func TestDurabilityCrossCell(t *testing.T) {
	err := SetDurabilityPolicy("cross_cell", nil)
	require.NoError(t, err)

	promoteRule := PromotionRule(&topodatapb.Tablet{
		Type: topodatapb.TabletType_PRIMARY,
	})
	assert.Equal(t, promotionrule.Neutral, promoteRule)

	promoteRule = PromotionRule(&topodatapb.Tablet{
		Type: topodatapb.TabletType_REPLICA,
	})
	assert.Equal(t, promotionrule.Neutral, promoteRule)

	promoteRule = PromotionRule(&topodatapb.Tablet{
		Type: topodatapb.TabletType_RDONLY,
	})
	assert.Equal(t, promotionrule.MustNot, promoteRule)

	promoteRule = PromotionRule(&topodatapb.Tablet{
		Type: topodatapb.TabletType_SPARE,
	})
	assert.Equal(t, promotionrule.MustNot, promoteRule)
	assert.Equal(t, 1, PrimarySemiSyncFromTablet(nil))
	assert.Equal(t, false, ReplicaSemiSyncFromTablet(&topodatapb.Tablet{
		Type: topodatapb.TabletType_PRIMARY,
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
		},
	}, &topodatapb.Tablet{
		Type: topodatapb.TabletType_REPLICA,
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
		},
	}))
	assert.Equal(t, true, ReplicaSemiSyncFromTablet(&topodatapb.Tablet{
		Type: topodatapb.TabletType_PRIMARY,
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
		},
	}, &topodatapb.Tablet{
		Type: topodatapb.TabletType_REPLICA,
		Alias: &topodatapb.TabletAlias{
			Cell: "cell2",
		},
	}))
	assert.Equal(t, false, ReplicaSemiSyncFromTablet(&topodatapb.Tablet{
		Type: topodatapb.TabletType_PRIMARY,
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
		},
	}, &topodatapb.Tablet{
		Type: topodatapb.TabletType_EXPERIMENTAL,
		Alias: &topodatapb.TabletAlias{
			Cell: "cell2",
		},
	}))
}

func TestError(t *testing.T) {
	err := SetDurabilityPolicy("unknown", nil)
	assert.EqualError(t, err, "durability policy unknown not found")
}

func TestDurabilitySpecified(t *testing.T) {
	cellName := "cell"
	durabilityRules := newDurabilitySpecified(
		map[string]string{
			"cell-0000000000": string(promotionrule.Must),
			"cell-0000000001": string(promotionrule.Prefer),
			"cell-0000000002": string(promotionrule.Neutral),
			"cell-0000000003": string(promotionrule.PreferNot),
			"cell-0000000004": string(promotionrule.MustNot),
		})

	testcases := []struct {
		tablet        *topodatapb.Tablet
		promotionRule promotionrule.CandidatePromotionRule
	}{
		{
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: cellName,
					Uid:  0,
				},
			},
			promotionRule: promotionrule.MustNot,
		}, {
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: cellName,
					Uid:  1,
				},
			},
			promotionRule: promotionrule.Prefer,
		}, {
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: cellName,
					Uid:  2,
				},
			},
			promotionRule: promotionrule.Neutral,
		}, {
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: cellName,
					Uid:  3,
				},
			},
			promotionRule: promotionrule.PreferNot,
		}, {
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: cellName,
					Uid:  4,
				},
			},
			promotionRule: promotionrule.MustNot,
		},
	}

	for _, testcase := range testcases {
		t.Run(topoproto.TabletAliasString(testcase.tablet.Alias), func(t *testing.T) {
			rule := durabilityRules.promotionRule(testcase.tablet)
			assert.Equal(t, testcase.promotionRule, rule)
		})
	}
}
