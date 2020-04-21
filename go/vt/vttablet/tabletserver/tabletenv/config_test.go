/*
Copyright 2020 The Vitess Authors.

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

package tabletenv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestConfigParse(t *testing.T) {
	cfg := TabletConfig{
		OltpReadPool: ConnPoolConfig{
			Size:               16,
			TimeoutSeconds:     10,
			IdleTimeoutSeconds: 20,
			PrefillParallelism: 30,
			MaxWaiters:         40,
		},
	}
	gotBytes, err := yaml.Marshal(&cfg)
	require.NoError(t, err)
	wantBytes := `hotRowProtection: {}
olapReadPool: {}
oltp: {}
oltpReadPool:
  idleTimeoutSeconds: 20
  maxWaiters: 40
  prefillParallelism: 30
  size: 16
  timeoutSeconds: 10
txPool: {}
`
	assert.Equal(t, wantBytes, string(gotBytes))

	// Make sure TimeoutSeconds doesn't get overwritten.
	inBytes := []byte(`oltpReadPool:
  size: 16
  idleTimeoutSeconds: 20
  prefillParallelism: 30
  maxWaiters: 40
`)
	gotCfg := cfg
	err = yaml.Unmarshal(inBytes, &gotCfg)
	require.NoError(t, err)
	assert.Equal(t, cfg, gotCfg)
}

func TestDefaultConfig(t *testing.T) {
	gotBytes, err := yaml.Marshal(NewDefaultConfig())
	require.NoError(t, err)
	want := `cacheResultFields: true
consolidator: enable
hotRowProtection:
  maxConcurrency: 5
  maxGlobalQueueSize: 1000
  maxQueueSize: 20
  mode: disable
messagePostponeParallelism: 4
olapReadPool:
  idleTimeoutSeconds: 1800
  size: 200
oltp:
  maxRpws: 10000
  queryTimeoutSeconds: 30
  txTimeoutSeconds: 30
oltpReadPool:
  idleTimeoutSeconds: 1800
  maxWaiters: 5000
  size: 16
queryCacheSize: 5000
schemaReloadIntervalSeconds: 1800
streamBufferSize: 32768
txPool:
  idleTimeoutSeconds: 1800
  maxWaiters: 5000
  size: 20
  timeoutSeconds: 1
`
	assert.Equal(t, want, string(gotBytes))
}

func TestClone(t *testing.T) {
	cfg1 := &TabletConfig{
		OltpReadPool: ConnPoolConfig{
			Size:               16,
			TimeoutSeconds:     10,
			IdleTimeoutSeconds: 20,
			PrefillParallelism: 30,
			MaxWaiters:         40,
		},
	}
	cfg2 := cfg1.Clone()
	assert.Equal(t, cfg1, cfg2)
	cfg1.OltpReadPool.Size = 10
	assert.NotEqual(t, cfg1, cfg2)
}
