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

package discovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/proto/vtadmin"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

func TestDiscoverVTGate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		contents  []byte
		expected  *vtadminpb.VTGate
		tags      []string
		shouldErr bool
	}{
		{
			name:      "empty config",
			contents:  []byte(`{}`),
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "one gate",
			contents: []byte(`
				{
					"vtgates": [{
						"host": {
							"hostname": "127.0.0.1:12345"
						}
					}]
				}
			`),
			expected: &vtadmin.VTGate{
				Hostname: "127.0.0.1:12345",
			},
		},
		{
			name: "filtered by tags (one match)",
			contents: []byte(`
				{
					"vtgates": [
						{
							"host": {
								"hostname": "127.0.0.1:11111"
							},
							"tags": ["cell:cellA"]
						}, 
						{
							"host": {
								"hostname": "127.0.0.1:22222"
							},
							"tags": ["cell:cellB"]
						},
						{
							"host": {
								"hostname": "127.0.0.1:33333"
							},
							"tags": ["cell:cellA"]
						}
					]
				}
			`),
			expected: &vtadminpb.VTGate{
				Hostname: "127.0.0.1:22222",
			},
			tags: []string{"cell:cellB"},
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			disco := &StaticFileDiscovery{}
			err := disco.parseConfig(tt.contents)
			require.NoError(t, err)

			gate, err := disco.DiscoverVTGate(ctx, tt.tags)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, gate)
		})
	}
}

func TestDiscoverVTGates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		contents []byte
		tags     []string
		expected []*vtadminpb.VTGate
		// True if the test should produce an error on the DiscoverVTGates call
		shouldErr bool
		// True if the test should produce an error on the disco.parseConfig step
		shouldErrConfig bool
	}{
		{
			name:      "empty config",
			contents:  []byte(`{}`),
			expected:  []*vtadminpb.VTGate{},
			shouldErr: false,
		},
		{
			name: "no tags",
			contents: []byte(`
				{
					"vtgates": [
						{
							"host": {
								"hostname": "127.0.0.1:12345"
							}
						},
						{
							"host": {
								"hostname": "127.0.0.1:67890"
							}
						}
					]
				}
			`),
			expected: []*vtadminpb.VTGate{
				{Hostname: "127.0.0.1:12345"},
				{Hostname: "127.0.0.1:67890"},
			},
			shouldErr: false,
		},
		{
			name: "filtered by tags",
			contents: []byte(`
				{
					"vtgates": [
						{
							"host": {
								"hostname": "127.0.0.1:11111"
							},
							"tags": ["cell:cellA"]
						},
						{
							"host": {
								"hostname": "127.0.0.1:22222"
							},
							"tags": ["cell:cellB"]
						},
						{
							"host": {
								"hostname": "127.0.0.1:33333"
							},
							"tags": ["cell:cellA"]
						}
					]
				}
			`),
			tags: []string{"cell:cellA"},
			expected: []*vtadminpb.VTGate{
				{Hostname: "127.0.0.1:11111"},
				{Hostname: "127.0.0.1:33333"},
			},
			shouldErr: false,
		},
		{
			name: "filtered by multiple tags",
			contents: []byte(`
				{
					"vtgates": [
						{
							"host": {
								"hostname": "127.0.0.1:11111"
							},
							"tags": ["cell:cellA"]
						},
						{
							"host": {
								"hostname": "127.0.0.1:22222"
							},
							"tags": ["cell:cellA", "pool:poolZ"]
						},
						{
							"host": {
								"hostname": "127.0.0.1:33333"
							},
							"tags": ["pool:poolZ"]
						}
					]
				}
			`),
			tags: []string{"cell:cellA", "pool:poolZ"},
			expected: []*vtadminpb.VTGate{
				{Hostname: "127.0.0.1:22222"},
			},
			shouldErr: false,
		},
		{
			name: "invalid json",
			contents: []byte(`
				{
					"vtgates": "malformed"
				}
			`),
			tags:            []string{},
			shouldErr:       false,
			shouldErrConfig: true,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			disco := &StaticFileDiscovery{}

			err := disco.parseConfig(tt.contents)
			if tt.shouldErrConfig {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			gates, err := disco.DiscoverVTGates(ctx, tt.tags)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.ElementsMatch(t, tt.expected, gates)
		})
	}
}

func TestDiscoverVtctld(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		contents  []byte
		expected  *vtadminpb.Vtctld
		tags      []string
		shouldErr bool
	}{
		{
			name:      "empty config",
			contents:  []byte(`{}`),
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "one vtctld",
			contents: []byte(`
				{
					"vtctlds": [{
						"host": {
							"hostname": "127.0.0.1:12345"
						}
					}]
				}
			`),
			expected: &vtadmin.Vtctld{
				Hostname: "127.0.0.1:12345",
			},
		},
		{
			name: "filtered by tags (one match)",
			contents: []byte(`
				{
					"vtctlds": [
						{
							"host": {
								"hostname": "127.0.0.1:11111"
							},
							"tags": ["cell:cellA"]
						}, 
						{
							"host": {
								"hostname": "127.0.0.1:22222"
							},
							"tags": ["cell:cellB"]
						},
						{
							"host": {
								"hostname": "127.0.0.1:33333"
							},
							"tags": ["cell:cellA"]
						}
					]
				}
			`),
			expected: &vtadminpb.Vtctld{
				Hostname: "127.0.0.1:22222",
			},
			tags: []string{"cell:cellB"},
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			disco := &StaticFileDiscovery{}
			err := disco.parseConfig(tt.contents)
			require.NoError(t, err)

			vtctld, err := disco.DiscoverVtctld(ctx, tt.tags)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, vtctld)
		})
	}
}

func TestDiscoverVtctlds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		contents []byte
		tags     []string
		expected []*vtadminpb.Vtctld
		// True if the test should produce an error on the DiscoverVTGates call
		shouldErr bool
		// True if the test should produce an error on the disco.parseConfig step
		shouldErrConfig bool
	}{
		{
			name:      "empty config",
			contents:  []byte(`{}`),
			expected:  []*vtadminpb.Vtctld{},
			shouldErr: false,
		},
		{
			name: "no tags",
			contents: []byte(`
				{
					"vtctlds": [
						{
							"host": {
								"hostname": "127.0.0.1:12345"
							}
						},
						{
							"host": {
								"hostname": "127.0.0.1:67890"
							}
						}
					]
				}
			`),
			expected: []*vtadminpb.Vtctld{
				{Hostname: "127.0.0.1:12345"},
				{Hostname: "127.0.0.1:67890"},
			},
			shouldErr: false,
		},
		{
			name: "filtered by tags",
			contents: []byte(`
				{
					"vtctlds": [
						{
							"host": {
								"hostname": "127.0.0.1:11111"
							},
							"tags": ["cell:cellA"]
						},
						{
							"host": {
								"hostname": "127.0.0.1:22222"
							},
							"tags": ["cell:cellB"]
						},
						{
							"host": {
								"hostname": "127.0.0.1:33333"
							},
							"tags": ["cell:cellA"]
						}
					]
				}
			`),
			tags: []string{"cell:cellA"},
			expected: []*vtadminpb.Vtctld{
				{Hostname: "127.0.0.1:11111"},
				{Hostname: "127.0.0.1:33333"},
			},
			shouldErr: false,
		},
		{
			name: "filtered by multiple tags",
			contents: []byte(`
				{
					"vtctlds": [
						{
							"host": {
								"hostname": "127.0.0.1:11111"
							},
							"tags": ["cell:cellA"]
						},
						{
							"host": {
								"hostname": "127.0.0.1:22222"
							},
							"tags": ["cell:cellA", "pool:poolZ"]
						},
						{
							"host": {
								"hostname": "127.0.0.1:33333"
							},
							"tags": ["pool:poolZ"]
						}
					]
				}
			`),
			tags: []string{"cell:cellA", "pool:poolZ"},
			expected: []*vtadminpb.Vtctld{
				{Hostname: "127.0.0.1:22222"},
			},
			shouldErr: false,
		},
		{
			name: "invalid json",
			contents: []byte(`
				{
					"vtctlds": "malformed"
				}
			`),
			tags:            []string{},
			shouldErr:       false,
			shouldErrConfig: true,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			disco := &StaticFileDiscovery{}

			err := disco.parseConfig(tt.contents)
			if tt.shouldErrConfig {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			vtctlds, err := disco.DiscoverVtctlds(ctx, tt.tags)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.ElementsMatch(t, tt.expected, vtctlds)
		})
	}
}
