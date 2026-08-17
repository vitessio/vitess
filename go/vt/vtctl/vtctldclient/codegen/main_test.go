/*
Copyright 2026 The Vitess Authors.

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

package main

import (
	"fmt"
	"go/token"
	"go/types"
	"testing"

	"github.com/stretchr/testify/require"
)

func newNamedType(pkgPath string, pkgName string, name string) *types.Named {
	pkg := types.NewPackage(pkgPath, pkgName)
	obj := types.NewTypeName(token.NoPos, pkg, name, nil)
	return types.NewNamed(obj, types.NewStruct(nil, nil), nil)
}

// newGenericInstance builds an instantiation of a generic interface type
// shaped like grpc.ServerStreamingClient[Res any], applied to typeArgs.
func newGenericInstance(t *testing.T, typeArgs ...types.Type) *types.Named {
	t.Helper()

	pkg := types.NewPackage("google.golang.org/grpc", "grpc")
	obj := types.NewTypeName(token.NoPos, pkg, "ServerStreamingClient", nil)
	generic := types.NewNamed(obj, types.NewInterfaceType(nil, nil), nil)

	tparams := make([]*types.TypeParam, len(typeArgs))
	for i := range typeArgs {
		tparams[i] = types.NewTypeParam(types.NewTypeName(token.NoPos, pkg, fmt.Sprintf("T%d", i), nil), types.NewInterfaceType(nil, nil))
	}
	generic.SetTypeParams(tparams)

	inst, err := types.Instantiate(nil, generic, typeArgs, false)
	require.NoError(t, err)

	named, ok := inst.(*types.Named)
	require.True(t, ok, "expected *types.Named, got %T", inst)
	return named
}

func TestRenderTypeArgs(t *testing.T) {
	t.Run("non-generic type", func(t *testing.T) {
		named := newNamedType("vitess.io/vitess/go/vt/proto/vtctlservice", "vtctlservice", "Vtctld_BackupClient")

		typeArgs, argImports, err := renderTypeArgs(named)
		require.NoError(t, err)
		require.Empty(t, typeArgs)
		require.Empty(t, argImports)
	})

	t.Run("named type argument", func(t *testing.T) {
		arg := newNamedType("vitess.io/vitess/go/vt/proto/vtctldata", "vtctldata", "BackupResponse")
		inst := newGenericInstance(t, arg)

		typeArgs, argImports, err := renderTypeArgs(inst)
		require.NoError(t, err)
		require.Equal(t, "[vtctldatapb.BackupResponse]", typeArgs)
		require.Equal(t, []typeArgImport{{localImport: "vtctldatapb", pkgPath: "vitess.io/vitess/go/vt/proto/vtctldata"}}, argImports)
	})

	t.Run("pointer type argument", func(t *testing.T) {
		arg := types.NewPointer(newNamedType("vitess.io/vitess/go/vt/proto/vtctldata", "vtctldata", "BackupResponse"))
		inst := newGenericInstance(t, arg)

		typeArgs, argImports, err := renderTypeArgs(inst)
		require.NoError(t, err)
		require.Equal(t, "[*vtctldatapb.BackupResponse]", typeArgs)
		require.Equal(t, []typeArgImport{{localImport: "vtctldatapb", pkgPath: "vitess.io/vitess/go/vt/proto/vtctldata"}}, argImports)
	})

	t.Run("multiple type arguments", func(t *testing.T) {
		req := newNamedType("vitess.io/vitess/go/vt/proto/vtctldata", "vtctldata", "BackupRequest")
		res := newNamedType("vitess.io/vitess/go/vt/proto/vtctldata", "vtctldata", "BackupResponse")
		inst := newGenericInstance(t, req, res)

		typeArgs, argImports, err := renderTypeArgs(inst)
		require.NoError(t, err)
		require.Equal(t, "[vtctldatapb.BackupRequest, vtctldatapb.BackupResponse]", typeArgs)
		require.Len(t, argImports, 2)
	})

	t.Run("unsupported type argument", func(t *testing.T) {
		inst := newGenericInstance(t, types.Typ[types.String])

		_, _, err := renderTypeArgs(inst)
		require.ErrorContains(t, err, "expected a named type")
	})
}

func TestExtractLocalNamedType(t *testing.T) {
	t.Run("plain named type", func(t *testing.T) {
		named := newNamedType("vitess.io/vitess/go/vt/proto/vtctlservice", "vtctlservice", "Vtctld_BackupClient")
		v := types.NewVar(token.NoPos, nil, "stream", named)

		name, localImport, pkgPath, argImports, err := extractLocalNamedType(v)
		require.NoError(t, err)
		require.Equal(t, "Vtctld_BackupClient", name)
		require.Equal(t, "vtctlservicepb", localImport)
		require.Equal(t, "vitess.io/vitess/go/vt/proto/vtctlservice", pkgPath)
		require.Empty(t, argImports)
	})

	t.Run("instantiated generic type", func(t *testing.T) {
		arg := newNamedType("vitess.io/vitess/go/vt/proto/vtctldata", "vtctldata", "BackupResponse")
		inst := newGenericInstance(t, arg)
		v := types.NewVar(token.NoPos, nil, "stream", inst)

		name, localImport, pkgPath, argImports, err := extractLocalNamedType(v)
		require.NoError(t, err)
		require.Equal(t, "ServerStreamingClient[vtctldatapb.BackupResponse]", name)
		require.Equal(t, "grpc", localImport)
		require.Equal(t, "google.golang.org/grpc", pkgPath)
		require.Equal(t, []typeArgImport{{localImport: "vtctldatapb", pkgPath: "vitess.io/vitess/go/vt/proto/vtctldata"}}, argImports)
	})
}
