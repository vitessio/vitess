package cli

import (
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TabletAliasesFromPosArgs takes a list of positional (non-flag) arguments and
// converts them to tablet aliases.
func TabletAliasesFromPosArgs(args []string) ([]*topodatapb.TabletAlias, error) {
	aliases := make([]*topodatapb.TabletAlias, 0, len(args))

	for _, arg := range args {
		alias, err := topoproto.ParseTabletAlias(arg)
		if err != nil {
			return nil, err
		}

		aliases = append(aliases, alias)
	}

	return aliases, nil
}
