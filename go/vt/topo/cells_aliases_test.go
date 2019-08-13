package topo

import (
	"strings"
	"testing"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestValidateAlias(t *testing.T) {
	table := []struct {
		currentAliases map[string][]string
		newAliasName   string
		newAlias       []string
		wantErrMsg     string
	}{
		{
			currentAliases: map[string][]string{
				"alias1": {"cell_a", "cell_b"},
				"alias2": {"cell_c", "cell_d"},
			},
			newAliasName: "overlaps_alias1",
			newAlias:     []string{"cell_x", "cell_b"},
			wantErrMsg:   "alias1",
		},
		{
			currentAliases: map[string][]string{
				"alias1": {"cell_a", "cell_b"},
				"alias2": {"cell_c", "cell_d"},
			},
			newAliasName: "overlaps_alias2",
			newAlias:     []string{"cell_x", "cell_c"},
			wantErrMsg:   "alias2",
		},
		{
			currentAliases: map[string][]string{
				"alias1": {"cell_a", "cell_b"},
				"alias2": {"cell_c", "cell_d"},
			},
			newAliasName: "no_overlap",
			newAlias:     []string{"cell_x", "cell_y"},
			wantErrMsg:   "",
		},
		{
			currentAliases: map[string][]string{
				"overlaps_self": {"cell_a", "cell_b"},
				"alias2":        {"cell_c", "cell_d"},
			},
			newAliasName: "overlaps_self",
			newAlias:     []string{"cell_a", "cell_b", "cell_x"},
			wantErrMsg:   "",
		},
	}

	for _, test := range table {
		currentAliases := map[string]*topodatapb.CellsAlias{}
		for name, cells := range test.currentAliases {
			currentAliases[name] = &topodatapb.CellsAlias{Cells: cells}
		}
		newAlias := &topodatapb.CellsAlias{Cells: test.newAlias}

		gotErr := validateAlias(currentAliases, test.newAliasName, newAlias)
		if test.wantErrMsg == "" {
			// Expect success.
			if gotErr != nil {
				t.Errorf("validateAlias(%v) error = %q; want nil", test.newAliasName, gotErr.Error())
			}
		} else {
			// Expect failure.
			if gotErr == nil {
				t.Errorf("validateAlias(%v) error = nil; want non-nil", test.newAliasName)
			}
			if got, want := gotErr.Error(), test.wantErrMsg; !strings.Contains(got, want) {
				t.Errorf("validateAlias(%v) error = %q; want *%q*", test.newAliasName, got, want)
			}
		}
	}
}
