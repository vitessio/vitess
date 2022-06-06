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
	durability, err := GetDurabilityPolicy("none")
	require.NoError(t, err)

	promoteRule := PromotionRule(durability, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Type: topodatapb.TabletType_PRIMARY,
	})
	assert.Equal(t, promotionrule.Neutral, promoteRule)
	assert.Equal(t, promotionrule.MustNot, PromotionRule(durability, nil))

	promoteRule = PromotionRule(durability, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Type: topodatapb.TabletType_REPLICA,
	})
	assert.Equal(t, promotionrule.Neutral, promoteRule)

	promoteRule = PromotionRule(durability, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Type: topodatapb.TabletType_RDONLY,
	})
	assert.Equal(t, promotionrule.MustNot, promoteRule)

	promoteRule = PromotionRule(durability, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Type: topodatapb.TabletType_SPARE,
	})
	assert.Equal(t, promotionrule.MustNot, promoteRule)
	assert.Equal(t, 0, SemiSyncAckers(durability, nil))
	assert.Equal(t, false, IsReplicaSemiSync(durability, nil, nil))
}

func TestDurabilitySemiSync(t *testing.T) {
	durability, err := GetDurabilityPolicy("semi_sync")
	require.NoError(t, err)

	promoteRule := PromotionRule(durability, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Type: topodatapb.TabletType_PRIMARY,
	})
	assert.Equal(t, promotionrule.Neutral, promoteRule)

	promoteRule = PromotionRule(durability, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Type: topodatapb.TabletType_REPLICA,
	})
	assert.Equal(t, promotionrule.Neutral, promoteRule)

	promoteRule = PromotionRule(durability, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Type: topodatapb.TabletType_RDONLY,
	})
	assert.Equal(t, promotionrule.MustNot, promoteRule)

	promoteRule = PromotionRule(durability, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Type: topodatapb.TabletType_SPARE,
	})
	assert.Equal(t, promotionrule.MustNot, promoteRule)
	assert.Equal(t, 1, SemiSyncAckers(durability, nil))
	assert.Equal(t, true, IsReplicaSemiSync(durability, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  101,
		},
		Type: topodatapb.TabletType_PRIMARY,
	}, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Type: topodatapb.TabletType_REPLICA,
	}))
	assert.Equal(t, false, IsReplicaSemiSync(durability, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  101,
		},
		Type: topodatapb.TabletType_PRIMARY,
	}, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Type: topodatapb.TabletType_EXPERIMENTAL,
	}))
}

func TestDurabilityCrossCell(t *testing.T) {
	durability, err := GetDurabilityPolicy("cross_cell")
	require.NoError(t, err)

	promoteRule := PromotionRule(durability, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Type: topodatapb.TabletType_PRIMARY,
	})
	assert.Equal(t, promotionrule.Neutral, promoteRule)

	promoteRule = PromotionRule(durability, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Type: topodatapb.TabletType_REPLICA,
	})
	assert.Equal(t, promotionrule.Neutral, promoteRule)

	promoteRule = PromotionRule(durability, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Type: topodatapb.TabletType_RDONLY,
	})
	assert.Equal(t, promotionrule.MustNot, promoteRule)

	promoteRule = PromotionRule(durability, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Type: topodatapb.TabletType_SPARE,
	})
	assert.Equal(t, promotionrule.MustNot, promoteRule)
	assert.Equal(t, 1, SemiSyncAckers(durability, nil))
	assert.Equal(t, false, IsReplicaSemiSync(durability, &topodatapb.Tablet{
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
	assert.Equal(t, true, IsReplicaSemiSync(durability, &topodatapb.Tablet{
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
	assert.Equal(t, false, IsReplicaSemiSync(durability, &topodatapb.Tablet{
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
	_, err := GetDurabilityPolicy("unknown")
	assert.EqualError(t, err, "durability policy unknown not found")
}

func TestDurabilityTest(t *testing.T) {
	cellName := "zone2"
	durabilityRules := &durabilityTest{}

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
				Type: topodatapb.TabletType_SPARE,
			},
			promotionRule: promotionrule.MustNot,
		}, {
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: cellName,
					Uid:  200,
				},
			},
			promotionRule: promotionrule.Prefer,
		}, {
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: cellName,
					Uid:  2,
				},
				Type: topodatapb.TabletType_PRIMARY,
			},
			promotionRule: promotionrule.Neutral,
		}, {
			tablet: &topodatapb.Tablet{
				Alias: &topodatapb.TabletAlias{
					Cell: cellName,
					Uid:  4,
				},
				Type: topodatapb.TabletType_BACKUP,
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
