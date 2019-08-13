package topo

import (
	"testing"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestOverlappingAlias(t *testing.T) {
	table := []struct {
		currentAliases map[string][]string
		newAliasName   string
		newAlias       []string
		want           string
	}{
		{
			currentAliases: map[string][]string{
				"alias1": {"cell_a", "cell_b"},
				"alias2": {"cell_c", "cell_d"},
			},
			newAliasName: "overlaps_alias1",
			newAlias:     []string{"cell_x", "cell_b"},
			want:         "alias1",
		},
		{
			currentAliases: map[string][]string{
				"alias1": {"cell_a", "cell_b"},
				"alias2": {"cell_c", "cell_d"},
			},
			newAliasName: "overlaps_alias2",
			newAlias:     []string{"cell_x", "cell_c"},
			want:         "alias2",
		},
		{
			currentAliases: map[string][]string{
				"alias1": {"cell_a", "cell_b"},
				"alias2": {"cell_c", "cell_d"},
			},
			newAliasName: "no_overlap",
			newAlias:     []string{"cell_x", "cell_y"},
			want:         "",
		},
		{
			currentAliases: map[string][]string{
				"overlaps_self": {"cell_a", "cell_b"},
				"alias2":        {"cell_c", "cell_d"},
			},
			newAliasName: "overlaps_self",
			newAlias:     []string{"cell_a", "cell_b", "cell_x"},
			want:         "",
		},
	}

	for _, test := range table {
		currentAliases := map[string]*topodatapb.CellsAlias{}
		for name, cells := range test.currentAliases {
			currentAliases[name] = &topodatapb.CellsAlias{Cells: cells}
		}
		newAlias := &topodatapb.CellsAlias{Cells: test.newAlias}

		got := overlappingAlias(currentAliases, test.newAliasName, newAlias)
		if got != test.want {
			t.Errorf("overlappingAlias(%v) = %q; want %q", test.newAliasName, got, test.want)
		}
	}
}
