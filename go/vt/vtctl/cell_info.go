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

	"context"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This file contains the Cells command group for vtctl.

const cellsGroupName = "Cells"

func init() {
	addCommandGroup(cellsGroupName)

	addCommand(cellsGroupName, command{
		"AddCellInfo",
		commandAddCellInfo,
		"[-server_address <addr>] [-root <root>] <cell>",
		"Registers a local topology service in a new cell by creating the CellInfo with the provided parameters. The address will be used to connect to the topology service, and we'll put Vitess data starting at the provided root."})

	addCommand(cellsGroupName, command{
		"UpdateCellInfo",
		commandUpdateCellInfo,
		"[-server_address <addr>] [-root <root>] <cell>",
		"Updates the content of a CellInfo with the provided parameters. If a value is empty, it is not updated. The CellInfo will be created if it doesn't exist."})

	addCommand(cellsGroupName, command{
		"DeleteCellInfo",
		commandDeleteCellInfo,
		"[-force] <cell>",
		"Deletes the CellInfo for the provided cell. The cell cannot be referenced by any Shard record."})

	addCommand(cellsGroupName, command{
		"GetCellInfoNames",
		commandGetCellInfoNames,
		"",
		"Lists all the cells for which we have a CellInfo object, meaning we have a local topology service registered."})

	addCommand(cellsGroupName, command{
		"GetCellInfo",
		commandGetCellInfo,
		"<cell>",
		"Prints a JSON representation of the CellInfo for a cell."})
}

func commandAddCellInfo(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	serverAddress := subFlags.String("server_address", "", "The address the topology server is using for that cell.")
	root := subFlags.String("root", "", "The root path the topology server is using for that cell.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if *root == "" {
		return fmt.Errorf("root must be non-empty")
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <cell> argument is required for the AddCellInfo command")
	}
	cell := subFlags.Arg(0)

	return wr.TopoServer().CreateCellInfo(ctx, cell, &topodatapb.CellInfo{
		ServerAddress: *serverAddress,
		Root:          *root,
	})
}

func commandUpdateCellInfo(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	serverAddress := subFlags.String("server_address", "", "The address the topology server is using for that cell.")
	root := subFlags.String("root", "", "The root path the topology server is using for that cell.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <cell> argument is required for the UpdateCellInfo command")
	}
	cell := subFlags.Arg(0)

	return wr.TopoServer().UpdateCellInfoFields(ctx, cell, func(ci *topodatapb.CellInfo) error {
		if (*serverAddress == "" || ci.ServerAddress == *serverAddress) &&
			(*root == "" || ci.Root == *root) {
			return topo.NewError(topo.NoUpdateNeeded, cell)
		}
		if *serverAddress != "" {
			ci.ServerAddress = *serverAddress
		}
		if *root != "" {
			ci.Root = *root
		}
		return nil
	})
}

func commandDeleteCellInfo(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	force := subFlags.Bool("force", false, "Proceeds even if the cell's topology server cannot be reached. The assumption is that you turned down the entire cell, and just need to update the global topo data.")
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <cell> argument is required for the DeleteCellInfo command")
	}
	cell := subFlags.Arg(0)

	return wr.TopoServer().DeleteCellInfo(ctx, cell, *force)
}

func commandGetCellInfoNames(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 0 {
		return fmt.Errorf("GetCellInfoNames command takes no parameter")
	}
	names, err := wr.TopoServer().GetCellInfoNames(ctx)
	if err != nil {
		return err
	}
	wr.Logger().Printf("%v\n", strings.Join(names, "\n"))
	return nil
}

func commandGetCellInfo(ctx context.Context, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) error {
	if err := subFlags.Parse(args); err != nil {
		return err
	}
	if subFlags.NArg() != 1 {
		return fmt.Errorf("the <cell> argument is required for the GetCellInfo command")
	}

	// We use a strong read, because users using this command want the
	// latest data, and this is user-generated, not used in any
	// automated process.
	cell := subFlags.Arg(0)
	ci, err := wr.TopoServer().GetCellInfo(ctx, cell, true /*strongRead*/)
	if err != nil {
		return err
	}
	return printJSON(wr.Logger(), ci)
}
