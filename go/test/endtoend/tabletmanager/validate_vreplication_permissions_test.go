package tabletmanager

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	tmdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func TestValidateVReplicationPermissions_SucceedsWithValidPermissions(t *testing.T) {
	tablet := getTablet(primaryTablet.GrpcPort)

	req := &tmdatapb.ValidateVReplicationPermissionsRequest{}
	_, err := tmClient.ValidateVReplicationPermissions(t.Context(), tablet, req)
	require.NoError(t, err)
}

func TestValidateVReplicationPermissions_FailsWithoutSelectPermissions(t *testing.T) {
	tablet := getTablet(primaryTablet.GrpcPort)

	// Revoke SELECT permission on the _vt.vreplication table
	conn, err := mysql.Connect(t.Context(), &primaryTabletParams)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	_, err = conn.ExecuteFetch("revoke select on *.* from vt_filtered@localhost", 0, false)
	require.NoError(t, err)
	t.Cleanup(func() {
		// Restore the permission for other tests
		_, err = conn.ExecuteFetch("grant select on *.* to vt_filtered@localhost", 0, false)
		require.NoError(t, err)
	})

	req := &tmdatapb.ValidateVReplicationPermissionsRequest{}
	res, err := tmClient.ValidateVReplicationPermissions(t.Context(), tablet, req)
	require.NoError(t, err)
	require.False(t, res.Ok)
}

func TestValidateVReplicationPermissions_FailsWithoutInsertPermissions(t *testing.T) {
	tablet := getTablet(primaryTablet.GrpcPort)

	// Revoke INSERT permission on the _vt.vreplication table
	conn, err := mysql.Connect(t.Context(), &primaryTabletParams)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	_, err = conn.ExecuteFetch("revoke insert on *.* from vt_filtered@localhost", 0, false)
	require.NoError(t, err)
	t.Cleanup(func() {
		// Restore the permission for other tests
		_, err = conn.ExecuteFetch("grant insert on *.* to vt_filtered@localhost", 0, false)
		require.NoError(t, err)
	})

	req := &tmdatapb.ValidateVReplicationPermissionsRequest{}
	res, err := tmClient.ValidateVReplicationPermissions(t.Context(), tablet, req)
	require.NoError(t, err)
	require.False(t, res.Ok)
}

func TestValidateVReplicationPermissions_FailsWithoutUpdatePermissions(t *testing.T) {
	tablet := getTablet(primaryTablet.GrpcPort)

	// Revoke UPDATE permission on the _vt.vreplication table
	conn, err := mysql.Connect(t.Context(), &primaryTabletParams)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	_, err = conn.ExecuteFetch("revoke update on *.* from vt_filtered@localhost", 0, false)
	require.NoError(t, err)
	t.Cleanup(func() {
		// Restore the permission for other tests
		_, err = conn.ExecuteFetch("grant update on *.* to vt_filtered@localhost", 0, false)
		require.NoError(t, err)
	})

	req := &tmdatapb.ValidateVReplicationPermissionsRequest{}
	res, err := tmClient.ValidateVReplicationPermissions(t.Context(), tablet, req)
	require.NoError(t, err)
	require.False(t, res.Ok)
}

func TestValidateVReplicationPermissions_FailsWithoutDeletePermissions(t *testing.T) {
	tablet := getTablet(primaryTablet.GrpcPort)

	// Revoke DELETE permission on the _vt.vreplication table
	conn, err := mysql.Connect(t.Context(), &primaryTabletParams)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	_, err = conn.ExecuteFetch("revoke delete on *.* from vt_filtered@localhost", 0, false)
	require.NoError(t, err)
	t.Cleanup(func() {
		// Restore the permission for other tests
		_, err = conn.ExecuteFetch("grant delete on *.* to vt_filtered@localhost", 0, false)
		require.NoError(t, err)
	})

	req := &tmdatapb.ValidateVReplicationPermissionsRequest{}
	res, err := tmClient.ValidateVReplicationPermissions(t.Context(), tablet, req)
	require.NoError(t, err)
	require.False(t, res.Ok)
}

func TestValidateVReplicationPermissions_FailsIfUserCantLogin(t *testing.T) {
	tablet := getTablet(primaryTablet.GrpcPort)

	// Lock the user account to simulate some other error
	conn, err := mysql.Connect(t.Context(), &primaryTabletParams)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	_, err = conn.ExecuteFetch("alter user 'vt_filtered'@'localhost' account lock", 0, false)
	require.NoError(t, err)
	t.Cleanup(func() {
		// Restore the permission for other tests
		_, err = conn.ExecuteFetch("alter user 'vt_filtered'@'localhost' account unlock", 0, false)
		require.NoError(t, err)
	})

	req := &tmdatapb.ValidateVReplicationPermissionsRequest{}
	_, err = tmClient.ValidateVReplicationPermissions(t.Context(), tablet, req)

	// This is an unexpected error, so we receive an error back
	require.Error(t, err)
	require.Contains(t, err.Error(), "Access denied for user 'vt_filtered'@'localhost'")
}
