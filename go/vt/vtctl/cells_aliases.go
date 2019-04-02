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
	"flag"
	"fmt"
	"strings"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This file contains the CellsAliases command group for vtctl.

const cellsAliasesGroupName = "CellsAliases"

func init() {
	addCommandGroup(cellsAliasesGroupName)

	addCommand(cellsAliasesGroupName, command{
		"AddCellsAlias",
		commandAddCellsAlias,
		"[-cells <cell,cell2...>] <alias>",
		"Registers a local topology service in a new cell by creating the CellInfo with the provided parameters. The address will be used to connect to the topology service, and we'll put Vitess data starting at the provided root."})

	addCommand(cellsAliasesGroupName, command{
		"UpdateCellsAlias",
		commandUpdateCellsAlias,
		"[-cells <cell,cell2,...>] <alias>",
		"Updates the content of a CellInfo with the provided parameters. If a value is empty, it is not updated. The CellInfo will be created if it doesn't exist."})

	addCommand(cellsAliasesGroupName, command{
		"DeleteCellsAlias",
		commandDeleteCellsAlias,
		"<alias>",
		"Deletes the CellsAlias for the provided alias."})

	addCommand(cellsAliasesGroupName, command{
		"GetCellsAliases",
		commandGetCellsAliases,
		"",
		"Lists all the cells for which we have a CellInfo object, meaning we have a local topology service registered."})
}

func commandAddCellsAlias(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cellsString := subFlags.String("cells", "", "The address the topology server is using for that cell.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <alias> argument is required for the AddCellsAlias command")
	}

	cells := strings.Split(*cellsString, ",")
	for i, cell := range cells {
		cells[i] = strings.TrimSpace(cell)
	}

	alias := subFlags.Arg(0)

	return wr.TopoServer().CreateCellsAlias(ctx, alias, &topodatapb.CellsAlias{
		Cells: cells,
	})
}

func commandUpdateCellsAlias(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	cellsString := subFlags.String("cells", "", "The address the topology server is using for that cell.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <alias> argument is required for the UpdateCellsAlias command")
	}

	cells := strings.Split(*cellsString, ",")
	for i, cell := range cells {
		cells[i] = strings.TrimSpace(cell)
	}

	alias := subFlags.Arg(0)

	return wr.TopoServer().UpdateCellsAlias(ctx, alias, func(ca *topodatapb.CellsAlias) error {
		ca.Cells = cells
		return nil
	})
}

func commandDeleteCellsAlias(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <alias> argument is required for the DeleteCellsAlias command")
	}
	alias := subFlags.Arg(0)

	return wr.TopoServer().DeleteCellsAlias(ctx, alias)
}

func commandGetCellsAliases(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
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
