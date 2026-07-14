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

package tabletmanager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	tmdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

const vreplicationPermissionTimeout = 5 * time.Second

func TestValidateVReplicationPermissions_SucceedsWithValidPermissions(t *testing.T) {
	permissionsMu.Lock()
	defer permissionsMu.Unlock()
	tablet, err := primaryTablet.TabletProto(t.Context())
	require.NoError(t, err)

	req := &tmdatapb.ValidateVReplicationPermissionsRequest{}
	_, err = tmClient.ValidateVReplicationPermissions(t.Context(), tablet, req)
	require.NoError(t, err)
}

func TestValidateVReplicationPermissions_FailsWithoutSelectPermissions(t *testing.T) {
	permissionsMu.Lock()
	defer permissionsMu.Unlock()
	tablet, err := primaryTablet.TabletProto(t.Context())
	require.NoError(t, err)

	// Revoke SELECT permission on the _vt.vreplication table
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &primaryTabletParams)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err = conn.ExecuteFetch("revoke select on *.* from vt_filtered@localhost", 0, false)
		require.NoError(c, err)
	}, vreplicationPermissionTimeout*3, 200*time.Millisecond)
	t.Cleanup(func() {
		// Restore the permission for other tests
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			_, err = conn.ExecuteFetch("grant select on *.* to vt_filtered@localhost", 0, false)
			require.NoError(c, err)
		}, vreplicationPermissionTimeout*3, 200*time.Millisecond)
	})

	req := &tmdatapb.ValidateVReplicationPermissionsRequest{}
	var res *tmdatapb.ValidateVReplicationPermissionsResponse
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ictx, icancel := context.WithTimeout(ctx, vreplicationPermissionTimeout)
		defer icancel()
		res, err = tmClient.ValidateVReplicationPermissions(ictx, tablet, req)
		require.NoError(c, err)
		require.NotNil(c, res)
		require.False(c, res.Ok)
	}, vreplicationPermissionTimeout*3, 200*time.Millisecond)
}

func TestValidateVReplicationPermissions_FailsWithoutInsertPermissions(t *testing.T) {
	permissionsMu.Lock()
	defer permissionsMu.Unlock()
	tablet, err := primaryTablet.TabletProto(t.Context())
	require.NoError(t, err)

	// Revoke INSERT permission on the _vt.vreplication table
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &primaryTabletParams)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err = conn.ExecuteFetch("revoke insert on *.* from vt_filtered@localhost", 0, false)
		require.NoError(c, err)
	}, vreplicationPermissionTimeout*3, 200*time.Millisecond)
	t.Cleanup(func() {
		// Restore the permission for other tests
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			_, err = conn.ExecuteFetch("grant insert on *.* to vt_filtered@localhost", 0, false)
			require.NoError(c, err)
		}, vreplicationPermissionTimeout*3, 200*time.Millisecond)
	})

	req := &tmdatapb.ValidateVReplicationPermissionsRequest{}
	var res *tmdatapb.ValidateVReplicationPermissionsResponse
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ictx, icancel := context.WithTimeout(ctx, vreplicationPermissionTimeout)
		defer icancel()
		res, err = tmClient.ValidateVReplicationPermissions(ictx, tablet, req)
		require.NoError(c, err)
		require.NotNil(c, res)
		require.False(c, res.Ok)
	}, vreplicationPermissionTimeout*3, 200*time.Millisecond)
}

func TestValidateVReplicationPermissions_FailsWithoutUpdatePermissions(t *testing.T) {
	permissionsMu.Lock()
	defer permissionsMu.Unlock()
	tablet, err := primaryTablet.TabletProto(t.Context())
	require.NoError(t, err)

	// Revoke UPDATE permission on the _vt.vreplication table
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &primaryTabletParams)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err = conn.ExecuteFetch("revoke update on *.* from vt_filtered@localhost", 0, false)
		require.NoError(c, err)
	}, vreplicationPermissionTimeout*3, 200*time.Millisecond)
	t.Cleanup(func() {
		// Restore the permission for other tests
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			_, err = conn.ExecuteFetch("grant update on *.* to vt_filtered@localhost", 0, false)
			require.NoError(c, err)
		}, vreplicationPermissionTimeout*3, 200*time.Millisecond)
	})

	req := &tmdatapb.ValidateVReplicationPermissionsRequest{}
	var res *tmdatapb.ValidateVReplicationPermissionsResponse
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ictx, icancel := context.WithTimeout(ctx, vreplicationPermissionTimeout)
		defer icancel()
		res, err = tmClient.ValidateVReplicationPermissions(ictx, tablet, req)
		require.NoError(c, err)
		require.NotNil(c, res)
		require.False(c, res.Ok)
	}, vreplicationPermissionTimeout*3, 200*time.Millisecond)
}

func TestValidateVReplicationPermissions_FailsWithoutDeletePermissions(t *testing.T) {
	permissionsMu.Lock()
	defer permissionsMu.Unlock()
	tablet, err := primaryTablet.TabletProto(t.Context())
	require.NoError(t, err)

	// Revoke DELETE permission on the _vt.vreplication table
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &primaryTabletParams)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err = conn.ExecuteFetch("revoke delete on *.* from vt_filtered@localhost", 0, false)
		require.NoError(c, err)
	}, vreplicationPermissionTimeout*3, 200*time.Millisecond)
	t.Cleanup(func() {
		// Restore the permission for other tests
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			_, err = conn.ExecuteFetch("grant delete on *.* to vt_filtered@localhost", 0, false)
			require.NoError(c, err)
		}, vreplicationPermissionTimeout*3, 200*time.Millisecond)
	})

	req := &tmdatapb.ValidateVReplicationPermissionsRequest{}
	var res *tmdatapb.ValidateVReplicationPermissionsResponse
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ictx, icancel := context.WithTimeout(ctx, vreplicationPermissionTimeout)
		defer icancel()
		res, err = tmClient.ValidateVReplicationPermissions(ictx, tablet, req)
		require.NoError(c, err)
		require.NotNil(c, res)
		require.False(c, res.Ok)
	}, vreplicationPermissionTimeout*3, 200*time.Millisecond)
}

func TestValidateVReplicationPermissions_FailsIfUserCantLogin(t *testing.T) {
	permissionsMu.Lock()
	defer permissionsMu.Unlock()
	tablet, err := primaryTablet.TabletProto(t.Context())
	require.NoError(t, err)

	ctx := t.Context()
	// Lock the user account to simulate some other error
	conn, err := mysql.Connect(ctx, &primaryTabletParams)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err = conn.ExecuteFetch("alter user 'vt_filtered'@'localhost' account lock", 0, false)
		require.NoError(c, err)
	}, vreplicationPermissionTimeout*3, 200*time.Millisecond)
	t.Cleanup(func() {
		// Restore the permission for other tests
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			_, err = conn.ExecuteFetch("alter user 'vt_filtered'@'localhost' account unlock", 0, false)
			require.NoError(c, err)
		}, vreplicationPermissionTimeout*3, 200*time.Millisecond)
	})

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ictx, icancel := context.WithTimeout(ctx, vreplicationPermissionTimeout)
		defer icancel()
		req := &tmdatapb.ValidateVReplicationPermissionsRequest{}
		_, err = tmClient.ValidateVReplicationPermissions(ictx, tablet, req)

		// This is an unexpected error, so we receive an error back.
		require.Error(c, err)
		require.Contains(c, err.Error(), "Access denied for user 'vt_filtered'@'localhost'")
	}, vreplicationPermissionTimeout*3, 200*time.Millisecond)
}
