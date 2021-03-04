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
	pluginName := "fake"
	plugin := &dbddlTestFake{}
	databaseCreatorPlugins[pluginName] = plugin

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
