/*
Copyright 2023 The Vitess Authors.

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

package cli

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/srvtopo/fakesrvtopo"
	"vitess.io/vitess/go/vt/srvtopo/srvtopotest"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/utils"
)

// TestExecute exercises the cobra Execute() path (PreRunE → RunE) with different
// os.Args so we know the CLI layer behaves as expected.
func TestExecute(t *testing.T) {
	t.Parallel()

	// Save and restore os.Args so this test doesn't leak into others.
	args := append([]string{}, os.Args...)
	t.Cleanup(func() {
		os.Args = append([]string{}, args...)
	})

	Main.SetGlobalNormalizationFunc(utils.NormalizeUnderscoresToDashes)

	t.Run("help succeeds", func(t *testing.T) {
		os.Args = []string{"vtgate", "--help"}
		err := Main.Execute()
		require.NoError(t, err) // help should always succeed
	})

	t.Run("unknown flag returns error", func(t *testing.T) {
		os.Args = []string{"vtgate", "--unknown-flag"}
		err := Main.Execute()
		assert.Error(t, err) // cobra should reject unknown flags
	})
}

// TestMainFlagRegistration checks that the flags we care about are actually
// registered on Main (from init + servenv + plugins). If any of these are missing,
// the binary would be broken at runtime.
func TestMainFlagRegistration(t *testing.T) {
	require.NotNil(t, Main.Flags().Lookup("cell"), "cell flag should be registered")
	cellFlag := Main.Flags().Lookup("cell")
	require.Equal(t, "cell to use (required)", cellFlag.Usage)

	require.NotNil(t, Main.Flags().Lookup("tablet-types-to-wait"), "tablet-types-to-wait should be registered")
	require.NotNil(t, Main.Flags().Lookup("planner-version"), "planner-version should be registered")

	require.NotNil(t, Main.Flags().Lookup("port"), "servenv port flag should be on Main")
	require.NotNil(t, Main.Flags().Lookup("bind-address"), "servenv bind-address should be on Main")

	require.NotNil(t, Main.Flags().Lookup("mysql-auth-server-static-file"), "at least one auth plugin flag should be registered")
}

// TestMainCommandMetadata makes sure the cobra command is wired the way we expect:
// correct use string, help text, no positional args, and run hooks set.
func TestMainCommandMetadata(t *testing.T) {
	require.Equal(t, "vtgate", Main.Use)
	require.Contains(t, Main.Short, "stateless proxy")
	require.Contains(t, Main.Long, "MySQL Protocol")
	require.NotNil(t, Main.Args)
	// Main should accept no positional args and reject extras.
	require.NoError(t, Main.Args(Main, []string{}))
	require.Error(t, Main.Args(Main, []string{"extra"}))
	require.NotNil(t, Main.PreRunE)
	require.NotNil(t, Main.RunE)
	require.NotEmpty(t, Main.Version)
}

// CheckCellFlags tests cover validation of cell and cells_to_watch. We use
// memorytopo + fakes so we don't need a real topo. The len(cellsInTopo)==0
// branch isn't covered here—that would need a topo that returns [] from
// GetKnownCells, and memorytopo doesn't give us that.

func TestCheckCellFlags_NilServer(t *testing.T) {
	ctx := context.Background()
	err := CheckCellFlags(ctx, nil, "c1", "c1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "topo server cannot be nil") // nil server should be rejected
}

func TestCheckCellFlags_GetTopoServerError(t *testing.T) {
	ctx := context.Background()
	passthrough := srvtopotest.NewPassthroughSrvTopoServer()
	passthrough.TopoServerError = errors.New("topo unreachable")

	err := CheckCellFlags(ctx, passthrough, "c1", "c1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unable to create gateway") // GetTopoServer failure should be wrapped
	require.ErrorContains(t, err, "topo unreachable")
}

func TestCheckCellFlags_GetKnownCellsError(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "c1")
	defer ts.Close()
	fake := &fakesrvtopo.FakeSrvTopo{Ts: ts}

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()

	err := CheckCellFlags(cancelledCtx, fake, "c1", "c1")
	require.Error(t, err) // cancelled context should make GetKnownCells fail
}

func TestCheckCellFlags_EmptyCell(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "c1")
	defer ts.Close()
	fake := &fakesrvtopo.FakeSrvTopo{Ts: ts}

	err := CheckCellFlags(ctx, fake, "", "c1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "cell flag must be set") // empty cell should be rejected
}

func TestCheckCellFlags_CellNotInTopo(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "c1")
	defer ts.Close()
	fake := &fakesrvtopo.FakeSrvTopo{Ts: ts}

	err := CheckCellFlags(ctx, fake, "bad", "c1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist in topo") // cell should exist in topo
	require.Contains(t, err.Error(), "bad")
}

func TestCheckCellFlags_CellsToWatchInvalidCell(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "c1")
	defer ts.Close()
	fake := &fakesrvtopo.FakeSrvTopo{Ts: ts}

	err := CheckCellFlags(ctx, fake, "c1", "c1,bad")
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not valid") // cells_to_watch entries should be in topo
	require.Contains(t, err.Error(), "Available cells")
}

func TestCheckCellFlags_CellsToWatchEmpty(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "c1")
	defer ts.Close()
	fake := &fakesrvtopo.FakeSrvTopo{Ts: ts}

	err := CheckCellFlags(ctx, fake, "c1", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "cells_to_watch flag cannot be empty") // empty string should fail
}

func TestCheckCellFlags_CellsToWatchEmptyAfterSplit(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "c1")
	defer ts.Close()
	fake := &fakesrvtopo.FakeSrvTopo{Ts: ts}

	err := CheckCellFlags(ctx, fake, "c1", ",")
	require.Error(t, err)
	require.Contains(t, err.Error(), "cells_to_watch flag cannot be empty") // comma-only should count as empty
}

func TestCheckCellFlags_Success(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "c1", "c2")
	defer ts.Close()
	fake := &fakesrvtopo.FakeSrvTopo{Ts: ts}

	err := CheckCellFlags(ctx, fake, "c1", "c1")
	require.NoError(t, err)
}

func TestCheckCellFlags_SuccessMultipleCells(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "c1", "c2")
	defer ts.Close()
	fake := &fakesrvtopo.FakeSrvTopo{Ts: ts}

	err := CheckCellFlags(ctx, fake, "c1", "c1,c2")
	require.NoError(t, err)
}

