package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type dbddlTestFake struct {
	createCalled, dropCalled bool
}

func (d *dbddlTestFake) CreateDatabase(ctx context.Context, name string) error {
	d.createCalled = true
	return nil
}

func (d *dbddlTestFake) DropDatabase(ctx context.Context, name string) error {
	d.dropCalled = true
	return nil
}

var _ DBDDLPlugin = (*dbddlTestFake)(nil)

func TestDBDDLCreateExecute(t *testing.T) {
	pluginName := "createFake"
	plugin := &dbddlTestFake{}
	DBDDLRegister(pluginName, plugin)

	primitive := &DBDDL{
		name:   "ks",
		create: true,
	}

	vc := &loggingVCursor{dbDDLPlugin: pluginName}

	_, err := primitive.Execute(vc, nil, false)
	require.NoError(t, err)
	require.True(t, plugin.createCalled)
	require.False(t, plugin.dropCalled)
}

func TestDBDDLDropExecute(t *testing.T) {
	pluginName := "dropFake"
	plugin := &dbddlTestFake{}
	DBDDLRegister(pluginName, plugin)

	primitive := &DBDDL{name: "ks"}

	vc := &loggingVCursor{dbDDLPlugin: pluginName}

	_, err := primitive.Execute(vc, nil, false)
	require.NoError(t, err)
	require.False(t, plugin.createCalled)
	require.True(t, plugin.dropCalled)
}
