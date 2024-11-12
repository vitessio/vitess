/*
Copyright 2022 The Vitess Authors.

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

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUpdateConfigValuesFromFlags(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		// Restore the changes we make to the Config parameter
		defer func() {
			Config = newConfiguration()
		}()
		defaultConfig := newConfiguration()
		UpdateConfigValuesFromFlags()
		require.Equal(t, defaultConfig, Config)
	})

	t.Run("override waitReplicasTimeout", func(t *testing.T) {
		oldWaitReplicasTimeout := waitReplicasTimeout
		waitReplicasTimeout = 3*time.Minute + 4*time.Second
		// Restore the changes we make
		defer func() {
			Config = newConfiguration()
			waitReplicasTimeout = oldWaitReplicasTimeout
		}()

		testConfig := newConfiguration()
		testConfig.WaitReplicasTimeoutSeconds = 184
		UpdateConfigValuesFromFlags()
		require.Equal(t, testConfig, Config)
	})

	t.Run("override topoInformationRefreshDuration", func(t *testing.T) {
		oldTopoInformationRefreshDuration := topoInformationRefreshDuration
		topoInformationRefreshDuration = 1 * time.Second
		// Restore the changes we make
		defer func() {
			Config = newConfiguration()
			topoInformationRefreshDuration = oldTopoInformationRefreshDuration
		}()

		testConfig := newConfiguration()
		testConfig.TopoInformationRefreshSeconds = 1
		UpdateConfigValuesFromFlags()
		require.Equal(t, testConfig, Config)
	})

	t.Run("override recoveryPollDuration", func(t *testing.T) {
		oldRecoveryPollDuration := recoveryPollDuration
		recoveryPollDuration = 15 * time.Second
		// Restore the changes we make
		defer func() {
			Config = newConfiguration()
			recoveryPollDuration = oldRecoveryPollDuration
		}()

		testConfig := newConfiguration()
		testConfig.RecoveryPollSeconds = 15
		UpdateConfigValuesFromFlags()
		require.Equal(t, testConfig, Config)
	})
}
