package config

import (
	"testing"

	"github.com/stretchr/testify/require"

	test "vitess.io/vitess/go/vt/vtorc/external/golib/tests"
)

func init() {
	Config.HostnameResolveMethod = "none"
}

func TestRecoveryPeriodBlock(t *testing.T) {
	{
		c := newConfiguration()
		c.RecoveryPeriodBlockSeconds = 0
		c.RecoveryPeriodBlockMinutes = 0
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.RecoveryPeriodBlockSeconds, 0)
	}
	{
		c := newConfiguration()
		c.RecoveryPeriodBlockSeconds = 30
		c.RecoveryPeriodBlockMinutes = 1
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.RecoveryPeriodBlockSeconds, 30)
	}
	{
		c := newConfiguration()
		c.RecoveryPeriodBlockSeconds = 0
		c.RecoveryPeriodBlockMinutes = 2
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.RecoveryPeriodBlockSeconds, 120)
	}
	{
		c := newConfiguration()
		c.RecoveryPeriodBlockSeconds = 15
		c.RecoveryPeriodBlockMinutes = 0
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.RecoveryPeriodBlockSeconds, 15)
	}
}

func TestRaft(t *testing.T) {
	{
		c := newConfiguration()
		c.RaftBind = "1.2.3.4:1008"
		c.RaftDataDir = "/path/to/somewhere"
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
		test.S(t).ExpectEquals(c.RaftAdvertise, c.RaftBind)
	}
	{
		c := newConfiguration()
		c.RaftEnabled = true
		err := c.postReadAdjustments()
		test.S(t).ExpectNotNil(err)
	}
	{
		c := newConfiguration()
		c.RaftEnabled = true
		c.RaftDataDir = "/path/to/somewhere"
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
	}
	{
		c := newConfiguration()
		c.RaftEnabled = true
		c.RaftDataDir = "/path/to/somewhere"
		c.RaftBind = ""
		err := c.postReadAdjustments()
		test.S(t).ExpectNotNil(err)
	}
}

func TestHttpAdvertise(t *testing.T) {
	{
		c := newConfiguration()
		c.HTTPAdvertise = ""
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
	}
	{
		c := newConfiguration()
		c.HTTPAdvertise = "http://127.0.0.1:1234"
		err := c.postReadAdjustments()
		test.S(t).ExpectNil(err)
	}
	{
		c := newConfiguration()
		c.HTTPAdvertise = "http://127.0.0.1"
		err := c.postReadAdjustments()
		test.S(t).ExpectNotNil(err)
	}
	{
		c := newConfiguration()
		c.HTTPAdvertise = "127.0.0.1:1234"
		err := c.postReadAdjustments()
		test.S(t).ExpectNotNil(err)
	}
	{
		c := newConfiguration()
		c.HTTPAdvertise = "http://127.0.0.1:1234/mypath"
		err := c.postReadAdjustments()
		test.S(t).ExpectNotNil(err)
	}
}

// TestOrchestratorToVTOrcBackwardCompatibility tests that the VTOrc configurations
// that are introduced dur to Orchestrator to VTOrc rename are backward compatibility
func TestOrchestratorToVTOrcBackwardCompatibility(t *testing.T) {
	t.Run("MySQLOrchestratorHost", func(t *testing.T) {
		c := newConfiguration()
		c.MySQLOrchestratorHost = "config val"
		err := c.postReadAdjustments()
		require.NoError(t, err)
		require.Equal(t, c.MySQLOrchestratorHost, c.MySQLVTOrcHost)
	})

	t.Run("MySQLOrchestratorMaxPoolConnections", func(t *testing.T) {
		c := newConfiguration()
		c.MySQLOrchestratorMaxPoolConnections = 19
		err := c.postReadAdjustments()
		require.NoError(t, err)
		require.Equal(t, c.MySQLOrchestratorMaxPoolConnections, c.MySQLVTOrcMaxPoolConnections)
	})

	t.Run("MySQLOrchestratorPort", func(t *testing.T) {
		c := newConfiguration()
		c.MySQLOrchestratorPort = 17
		err := c.postReadAdjustments()
		require.NoError(t, err)
		require.Equal(t, c.MySQLOrchestratorPort, c.MySQLVTOrcPort)
	})

	t.Run("MySQLOrchestratorDatabase", func(t *testing.T) {
		c := newConfiguration()
		c.MySQLOrchestratorDatabase = "config val"
		err := c.postReadAdjustments()
		require.NoError(t, err)
		require.Equal(t, c.MySQLOrchestratorDatabase, c.MySQLVTOrcDatabase)
	})

	t.Run("MySQLOrchestratorUser", func(t *testing.T) {
		c := newConfiguration()
		c.MySQLOrchestratorUser = "config val"
		err := c.postReadAdjustments()
		require.NoError(t, err)
		require.Equal(t, c.MySQLOrchestratorUser, c.MySQLVTOrcUser)
	})

	t.Run("MySQLOrchestratorPassword", func(t *testing.T) {
		c := newConfiguration()
		c.MySQLOrchestratorPassword = "config val"
		err := c.postReadAdjustments()
		require.NoError(t, err)
		require.Equal(t, c.MySQLOrchestratorPassword, c.MySQLVTOrcPassword)
	})

	t.Run("MySQLOrchestratorSSLPrivateKeyFile", func(t *testing.T) {
		c := newConfiguration()
		c.MySQLOrchestratorSSLPrivateKeyFile = "config val"
		err := c.postReadAdjustments()
		require.NoError(t, err)
		require.Equal(t, c.MySQLOrchestratorSSLPrivateKeyFile, c.MySQLVTOrcSSLPrivateKeyFile)
	})

	t.Run("MySQLOrchestratorSSLCertFile", func(t *testing.T) {
		c := newConfiguration()
		c.MySQLOrchestratorSSLCertFile = "config val"
		err := c.postReadAdjustments()
		require.NoError(t, err)
		require.Equal(t, c.MySQLOrchestratorSSLCertFile, c.MySQLVTOrcSSLCertFile)
	})

	t.Run("MySQLOrchestratorSSLCAFile", func(t *testing.T) {
		c := newConfiguration()
		c.MySQLOrchestratorSSLCAFile = "config val"
		err := c.postReadAdjustments()
		require.NoError(t, err)
		require.Equal(t, c.MySQLOrchestratorSSLCAFile, c.MySQLVTOrcSSLCAFile)
	})

	t.Run("MySQLOrchestratorSSLSkipVerify", func(t *testing.T) {
		c := newConfiguration()
		c.MySQLOrchestratorSSLSkipVerify = true
		err := c.postReadAdjustments()
		require.NoError(t, err)
		require.Equal(t, c.MySQLOrchestratorSSLSkipVerify, c.MySQLVTOrcSSLSkipVerify)
	})

	t.Run("MySQLOrchestratorUseMutualTLS", func(t *testing.T) {
		c := newConfiguration()
		c.MySQLOrchestratorUseMutualTLS = true
		err := c.postReadAdjustments()
		require.NoError(t, err)
		require.Equal(t, c.MySQLOrchestratorUseMutualTLS, c.MySQLVTOrcUseMutualTLS)
	})

	t.Run("MySQLOrchestratorReadTimeoutSeconds", func(t *testing.T) {
		c := newConfiguration()
		c.MySQLOrchestratorReadTimeoutSeconds = 37
		err := c.postReadAdjustments()
		require.NoError(t, err)
		require.Equal(t, c.MySQLOrchestratorReadTimeoutSeconds, c.MySQLVTOrcReadTimeoutSeconds)
	})

	t.Run("MySQLOrchestratorRejectReadOnly", func(t *testing.T) {
		c := newConfiguration()
		c.MySQLOrchestratorRejectReadOnly = true
		err := c.postReadAdjustments()
		require.NoError(t, err)
		require.Equal(t, c.MySQLOrchestratorRejectReadOnly, c.MySQLVTOrcRejectReadOnly)
	})
}
