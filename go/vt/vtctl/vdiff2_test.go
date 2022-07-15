package vtctl

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vdiff"
)

func TestGetStructNames(t *testing.T) {
	type s struct {
		A string
		B int64
	}
	got := getStructFieldNames(s{})
	want := []string{"A", "B"}
	require.EqualValues(t, want, got)
}

func TestBuildProgressReport(t *testing.T) {
	type args struct {
		summary       *vdiffSummary
		rowsToCompare int64
	}
	tests := []struct {
		name string
		args args
		want *vdiff.ProgressReport
	}{
		{
			name: "no progress",
			args: args{
				summary:       &vdiffSummary{RowsCompared: 0},
				rowsToCompare: 100,
			},
			want: &vdiff.ProgressReport{
				Percentage: 0,
				ETA:        "", // no ETA
			},
		},
		{
			name: "one third of the way",
			args: args{
				summary: &vdiffSummary{
					RowsCompared: 33,
					StartedAt:    time.Now().Add(-10 * time.Second).UTC().Format(vdiff.TimestampFormat),
				},
				rowsToCompare: 100,
			},
			want: &vdiff.ProgressReport{
				Percentage: 33,
				ETA:        time.Now().Add(20 * time.Second).UTC().Format(vdiff.TimestampFormat),
			},
		},
		{
			name: "half way",
			args: args{
				summary: &vdiffSummary{
					RowsCompared: 50,
					StartedAt:    time.Now().Add(-10 * time.Hour).UTC().Format(vdiff.TimestampFormat),
				},
				rowsToCompare: 100,
			},
			want: &vdiff.ProgressReport{
				Percentage: 50,
				ETA:        time.Now().Add(10 * time.Hour).UTC().Format(vdiff.TimestampFormat),
			},
		},
		{
			name: "full progress",
			args: args{
				summary: &vdiffSummary{
					RowsCompared: 100,
					CompletedAt:  time.Now().UTC().Format(vdiff.TimestampFormat),
				},
				rowsToCompare: 100,
			},
			want: &vdiff.ProgressReport{
				Percentage: 100,
				ETA:        time.Now().UTC().Format(vdiff.TimestampFormat),
			},
		},
		{
			name: "more than in I_S",
			args: args{
				summary: &vdiffSummary{
					RowsCompared: 100,
					CompletedAt:  time.Now().UTC().Format(vdiff.TimestampFormat),
				},
				rowsToCompare: 50,
			},
			want: &vdiff.ProgressReport{
				Percentage: 100,
				ETA:        time.Now().UTC().Format(vdiff.TimestampFormat),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buildProgressReport(tt.args.summary, tt.args.rowsToCompare)
			// We always check the percentage
			require.Equal(t, tt.want.Percentage, tt.args.summary.Progress.Percentage)

			// We only check the ETA if there is one
			if tt.want.ETA != "" {
				// Let's check that we're within 1 second to avoid flakes
				wt, err := time.Parse(vdiff.TimestampFormat, tt.want.ETA)
				require.NoError(t, err)
				var timeDiff float64
				if tt.want.Percentage == 100 {
					ct, err := time.Parse(vdiff.TimestampFormat, tt.args.summary.CompletedAt)
					require.NoError(t, err)
					timeDiff = math.Abs(ct.Sub(wt).Seconds())
				} else {
					st, err := time.Parse(vdiff.TimestampFormat, tt.args.summary.StartedAt)
					require.NoError(t, err)
					ct := float64(time.Now().UTC().Unix()-st.UTC().Unix()) * (100 / tt.want.Percentage)
					et, err := time.Parse(vdiff.TimestampFormat, tt.want.ETA)
					require.NoError(t, err)
					timeDiff = math.Abs(et.Sub(st).Seconds() - ct)
				}
				require.LessOrEqual(t, timeDiff, 1.0)
			}
		})
	}
}
