package promotionrule

import (
	"testing"

	"github.com/stretchr/testify/assert"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestPromotionRuleParseFromProto(t *testing.T) {
	testcases := []struct {
		in          topodatapb.PromotionRule
		out         CandidatePromotionRule
		errContains string
	}{
		{
			in:  topodatapb.PromotionRule_NONE,
			out: Neutral,
		},
		{

			in:  topodatapb.PromotionRule_NEUTRAL,
			out: Neutral,
		},
		{
			in:          topodatapb.PromotionRule_MUST,
			out:         Must,
			errContains: "ParseFromProto: MUST not supported yet",
		},
		{
			in:  topodatapb.PromotionRule_PREFER,
			out: Prefer,
		},
		{
			in:  topodatapb.PromotionRule_PREFER_NOT,
			out: PreferNot,
		},
		{
			in:  topodatapb.PromotionRule_MUST_NOT,
			out: MustNot,
		},
		{
			in:          999,
			out:         Neutral,
			errContains: "unsupported promotion rule",
		},
	}

	for _, testcase := range testcases {
		testcase := testcase
		t.Run(testcase.in.String(), func(t *testing.T) {
			t.Parallel()
			rule, err := ParseFromProto(testcase.in)
			if testcase.errContains != "" {
				assert.ErrorContains(t, err, testcase.errContains)
			}
			assert.Equal(t, testcase.out, rule)
		})
	}
}
