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

package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

type dbddlTestFake struct {
	createCalled, dropCalled bool
	sleep                    int
}

func (d *dbddlTestFake) CreateDatabase(ctx context.Context, name string) error {
	if d.sleep > 0 {
		time.Sleep(time.Duration(d.sleep) * time.Second)
	}
	d.createCalled = true
	return nil
}

func (d *dbddlTestFake) DropDatabase(ctx context.Context, name string) error {
	if d.sleep > 0 {
		time.Sleep(time.Duration(d.sleep) * time.Second)
	}
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

	_, err := primitive.TryExecute(t.Context(), vc, nil, false)
	require.NoError(t, err)
	require.True(t, plugin.createCalled)
	require.False(t, plugin.dropCalled)
}

func TestDBDDLDropExecute(t *testing.T) {
	pluginName := "dropFake"
	plugin := &dbddlTestFake{}
	DBDDLRegister(pluginName, plugin)

	primitive := &DBDDL{name: "ks"}

	vc := &loggingVCursor{dbDDLPlugin: pluginName, ksAvailable: false}

	_, err := primitive.TryExecute(t.Context(), vc, nil, false)
	require.NoError(t, err)
	require.False(t, plugin.createCalled)
	require.True(t, plugin.dropCalled)
}

func TestDBDDLTimeout(t *testing.T) {
	pluginName := "timeoutFake"
	plugin := &dbddlTestFake{sleep: 2}
	DBDDLRegister(pluginName, plugin)

	primitive := &DBDDL{name: "ks", create: true, queryTimeout: 100}
	vc := &loggingVCursor{dbDDLPlugin: pluginName, shardErr: errors.New("db not available")}
	_, err := primitive.TryExecute(t.Context(), vc, nil, false)
	require.EqualError(t, err, "could not validate create database: destination not resolved")

	primitive = &DBDDL{name: "ks", queryTimeout: 100}
	vc = &loggingVCursor{dbDDLPlugin: pluginName, ksAvailable: true}
	_, err = primitive.TryExecute(t.Context(), vc, nil, false)
	assert.EqualError(t, err, "could not validate drop database: keyspace still available in vschema")
}
