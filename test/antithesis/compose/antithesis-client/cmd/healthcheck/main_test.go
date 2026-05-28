package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckOnceHealthyShards(t *testing.T) {
	primary := tabletStats{
		Tablet:  tabletInfo{Keyspace: "test-customer", Shard: "-80", Type: 1},
		Target:  tabletTarget{Keyspace: "test-customer", Shard: "-80", TabletType: 1},
		Serving: true,
	}
	replica := tabletStats{
		Tablet:  tabletInfo{Keyspace: "test-customer", Shard: "-80", Type: 2},
		Target:  tabletTarget{Keyspace: "test-customer", Shard: "-80", TabletType: 2},
		Serving: true,
	}
	statuses := []tabletCacheStatus{
		{Cell: "zone1", Target: tabletTarget{Keyspace: "test-customer", Shard: "-80", TabletType: 1}, TabletsStats: []tabletStats{primary, replica}},
	}
	server := newHealthCheckServer(t, statuses)

	ok, err := checkOnce(server.URL, "test")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestCheckOnceMissingReplica(t *testing.T) {
	primary := tabletStats{
		Tablet:  tabletInfo{Keyspace: "test-customer", Shard: "-80", Type: 1},
		Target:  tabletTarget{Keyspace: "test-customer", Shard: "-80", TabletType: 1},
		Serving: true,
	}
	statuses := []tabletCacheStatus{
		{Cell: "zone1", Target: tabletTarget{Keyspace: "test-customer", Shard: "-80", TabletType: 1}, TabletsStats: []tabletStats{primary}},
	}
	server := newHealthCheckServer(t, statuses)

	ok, err := checkOnce(server.URL, "test")
	require.Error(t, err)
	assert.False(t, ok)
}

func TestCheckOnceUnhealthyPrimary(t *testing.T) {
	lastErr := "repl lag"
	primary := tabletStats{
		Tablet:    tabletInfo{Keyspace: "test-customer", Shard: "-80", Type: 1},
		Target:    tabletTarget{Keyspace: "test-customer", Shard: "-80", TabletType: 1},
		Serving:   true,
		LastError: &lastErr,
	}
	replica := tabletStats{
		Tablet:  tabletInfo{Keyspace: "test-customer", Shard: "-80", Type: 2},
		Target:  tabletTarget{Keyspace: "test-customer", Shard: "-80", TabletType: 2},
		Serving: true,
	}
	statuses := []tabletCacheStatus{
		{Cell: "zone1", Target: tabletTarget{Keyspace: "test-customer", Shard: "-80", TabletType: 1}, TabletsStats: []tabletStats{primary, replica}},
	}
	server := newHealthCheckServer(t, statuses)

	ok, err := checkOnce(server.URL, "test")
	require.Error(t, err)
	assert.False(t, ok)
}

func newHealthCheckServer(t *testing.T, statuses []tabletCacheStatus) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/health-check/cell/test" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(statuses))
	}))
}
