package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/sync2"

	"github.com/stretchr/testify/require"
)

type dbddlTestFake struct {
	pluginName, oldKS string

	createCalled, dropCalled bool

	setTargetError error

	noopVCursor
	targetSet string
}

func (d *dbddlTestFake) GetDBDDLPluginName() string {
	return d.pluginName
}
func (d *dbddlTestFake) CreateDatabase(ctx context.Context, name string) error {
	d.createCalled = true
	return nil
}

func (d *dbddlTestFake) DropDatabase(ctx context.Context, name string) error {
	d.dropCalled = true
	return nil
}

func (d *dbddlTestFake) Session() SessionActions {
	return d
}
func (d *dbddlTestFake) SetTarget(t string) error {
	d.targetSet = t
	return d.setTargetError
}

var _ VCursor = (*dbddlTestFake)(nil)
var _ SessionActions = (*dbddlTestFake)(nil)
var _ DBDDLPlugin = (*dbddlTestFake)(nil)

func TestDBDDLCreateExecute(t *testing.T) {
	fake := &dbddlTestFake{
		pluginName:     "plugin",
		oldKS:          "ks",
		setTargetError: fmt.Errorf("oh noes"),
	}

	databaseCreatorPlugins["plugin"] = fake

	primitive := &DBDDL{
		name:   "basedata",
		create: true,
	}

	canStop := sync2.AtomicBool{}
	canStop.Set(false)

	go func() {
		_, err := primitive.Execute(fake, nil, false)
		require.NoError(t, err)
		require.True(t, canStop.Get())
	}()

	time.Sleep(1 * time.Second)
	canStop.Set(true)
	fake.setTargetError = nil
}
