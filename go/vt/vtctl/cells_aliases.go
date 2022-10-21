/*
Copyright 2019 The Vitess Authors.

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

package vtctl

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// This file contains the CellsAliases command group for vtctl.

const cellsAliasesGroupName = "CellsAliases"

func init() {
	addCommandGroup(cellsAliasesGroupName)

	addCommand(cellsAliasesGroupName, command{
		name:   "AddCellsAlias",
		method: commandAddCellsAlias,
		params: "[--cells <cell,cell2...>] <alias>",
		help:   "Defines a group of cells within which replica/rdonly traffic can be routed across cells. Between cells that are not in the same group (alias), only primary traffic can be routed.",
	})

	addCommand(cellsAliasesGroupName, command{
		name:   "UpdateCellsAlias",
		method: commandUpdateCellsAlias,
		params: "[--cells <cell,cell2,...>] <alias>",
		help:   "Updates the content of a CellsAlias with the provided parameters. If a value is empty, it is not updated. The CellsAlias will be created if it doesn't exist.",
	})

	addCommand(cellsAliasesGroupName, command{
		name:   "DeleteCellsAlias",
		method: commandDeleteCellsAlias,
		params: "<alias>",
		help:   "Deletes the CellsAlias for the provided alias.",
	})

	addCommand(cellsAliasesGroupName, command{
		name:   "GetCellsAliases",
		method: commandGetCellsAliases,
		params: "",
		help:   "Lists all the cells for which we have a CellsAlias object.",
	})
}

func commandAddCellsAlias(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	cells := subFlags.StringSlice("cells", nil, "The list of cell names that are members of this alias.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <alias> argument is required for the AddCellsAlias command")
	}

	for i, cell := range *cells {
		(*cells)[i] = strings.TrimSpace(cell)
	}

	alias := subFlags.Arg(0)
	_, err := wr.VtctldServer().AddCellsAlias(ctx, &vtctldatapb.AddCellsAliasRequest{
		Name:  alias,
		Cells: *cells,
	})
	return err
}

func commandUpdateCellsAlias(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	cells := subFlags.StringSlice("cells", nil, "The list of cell names that are members of this alias.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <alias> argument is required for the UpdateCellsAlias command")
	}

	for i, cell := range *cells {
		(*cells)[i] = strings.TrimSpace(cell)
	}

	alias := subFlags.Arg(0)
	_, err := wr.VtctldServer().UpdateCellsAlias(ctx, &vtctldatapb.UpdateCellsAliasRequest{
		Name: alias,
		CellsAlias: &topodatapb.CellsAlias{
			Cells: *cells,
		},
	})
	return err
}

func commandDeleteCellsAlias(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <alias> argument is required for the DeleteCellsAlias command")
	}
	alias := subFlags.Arg(0)

	_, err := wr.VtctldServer().DeleteCellsAlias(ctx, &vtctldatapb.DeleteCellsAliasRequest{
		Name: alias,
	})
	return err
}

func commandGetCellsAliases(ctx context.Context, wr *wrangler.Wrangler, subFlags *pflag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 0 {
		return fmt.Errorf("GetCellsAliases command takes no parameter")
	}
	aliases, err := wr.TopoServer().GetCellsAliases(ctx, true /*strongRead*/)
	if err != nil {
		return err
	}
	return printJSON(wr.Logger(), aliases)
}
