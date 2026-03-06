package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
)

type healthCounts struct {
	primaries int
	replicas  int
}

type tabletTarget struct {
	Keyspace   string `json:"keyspace"`
	Shard      string `json:"shard"`
	TabletType int    `json:"tablet_type"`
}

type tabletInfo struct {
	Keyspace string `json:"keyspace"`
	Shard    string `json:"shard"`
	Type     int    `json:"type"`
}

type tabletStats struct {
	Tablet    tabletInfo   `json:"Tablet"`
	Target    tabletTarget `json:"Target"`
	Serving   bool         `json:"Serving"`
	LastError *string      `json:"LastError"`
}

type tabletCacheStatus struct {
	Cell         string        `json:"Cell"`
	Target       tabletTarget  `json:"Target"`
	TabletsStats []tabletStats `json:"TabletsStats"`
}

const defaultTimeout = 30 * time.Second

func main() {
	defaultVtgate := getenv("VTGATE_ADDR", "http://vtgate:8080")
	defaultCell := getenv("VTCTLD_CELL", "test")
	var (
		vtgateAddr   = flag.String("vtgate", defaultVtgate, "vtgate base address")
		cell         = flag.String("cell", defaultCell, "cell to query")
		timeout      = flag.Duration("timeout", defaultTimeout, "timeout for health check")
		pollInterval = flag.Duration("poll-interval", 2*time.Second, "poll interval for health check")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if err := checkEventually(ctx, *vtgateAddr, *cell, *pollInterval); err != nil {
		assert.Unreachable("Vitess cluster health did not recover within timeout", map[string]any{"error": err.Error()})
		fmt.Printf("health check failed: %v\n", err)
		os.Exit(1)
	}
}

func checkEventually(ctx context.Context, vtgateAddr string, cell string, pollInterval time.Duration) error {
	var lastErr error
	for {
		ok, err := checkOnce(vtgateAddr, cell)
		if err != nil {
			lastErr = err
		} else if ok {
			assert.Always(true, "Vitess shard has primary and replica", map[string]any{"vtgate": vtgateAddr, "cell": cell})
			return nil
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return lastErr
			}
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

func checkOnce(vtgateAddr string, cell string) (bool, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	listURL := fmt.Sprintf("%s/api/health-check/cell/%s", vtgateAddr, cell)
	resp, err := client.Get(listURL)
	if err != nil {
		return false, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, nil
	}
	var cacheStatuses []tabletCacheStatus
	if err := json.NewDecoder(resp.Body).Decode(&cacheStatuses); err != nil {
		return false, err
	}
	if len(cacheStatuses) == 0 {
		return false, errors.New("health check list is empty")
	}
	counts := map[string]*healthCounts{}
	for _, cacheStatus := range cacheStatuses {
		for _, tablet := range cacheStatus.TabletsStats {
			if tablet.Tablet.Keyspace == "" || tablet.Tablet.Shard == "" {
				continue
			}
			if !isHealthy(tablet) {
				continue
			}
			key := tablet.Tablet.Keyspace + ":" + tablet.Tablet.Shard
			entry := counts[key]
			if entry == nil {
				entry = &healthCounts{}
				counts[key] = entry
			}
			if isPrimary(tablet.Tablet.Type) {
				entry.primaries++
			}
			if isReplica(tablet.Tablet.Type) {
				entry.replicas++
			}
		}
	}
	if len(counts) == 0 {
		return false, errors.New("no shard health data found")
	}
	for key, entry := range counts {
		if entry.primaries < 1 || entry.replicas < 1 {
			return false, fmt.Errorf("shard %s does not have required healthy primary/replica", key)
		}
	}
	return true, nil
}

func isPrimary(t int) bool {
	return t == 1
}

func isReplica(t int) bool {
	return t == 2
}

func isHealthy(stats tabletStats) bool {
	if !stats.Serving {
		return false
	}
	if stats.LastError == nil {
		return true
	}
	return *stats.LastError == ""
}

func getenv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
