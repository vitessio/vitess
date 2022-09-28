package config

import (
	"testing"

	"github.com/stretchr/testify/require"
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
		require.NoError(t, err)
		require.EqualValues(t, 0, c.RecoveryPeriodBlockSeconds)
	}
	{
		c := newConfiguration()
		c.RecoveryPeriodBlockSeconds = 30
		c.RecoveryPeriodBlockMinutes = 1
		err := c.postReadAdjustments()
		require.NoError(t, err)
		require.EqualValues(t, 30, c.RecoveryPeriodBlockSeconds)
	}
	{
		c := newConfiguration()
		c.RecoveryPeriodBlockSeconds = 0
		c.RecoveryPeriodBlockMinutes = 2
		err := c.postReadAdjustments()
		require.NoError(t, err)
		require.EqualValues(t, 120, c.RecoveryPeriodBlockSeconds)
	}
	{
		c := newConfiguration()
		c.RecoveryPeriodBlockSeconds = 15
		c.RecoveryPeriodBlockMinutes = 0
		err := c.postReadAdjustments()
		require.NoError(t, err)
		require.EqualValues(t, 15, c.RecoveryPeriodBlockSeconds)
	}
}
