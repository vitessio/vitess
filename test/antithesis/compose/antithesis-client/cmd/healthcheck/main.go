package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
)

type healthCounts struct {
	primaries int
	replicas  int
}

type tabletAlias struct {
	Cell string `json:"cell"`
	UID  int    `json:"uid"`
}

type tabletRecord struct {
	Keyspace string      `json:"keyspace"`
	Shard    string      `json:"shard"`
	Type     interface{} `json:"type"`
}

const defaultTimeout = 30 * time.Second

func main() {
	defaultVtctld := getenv("VTCTLD_ADDR", "http://vtctld:8080")
	defaultCell := getenv("VTCTLD_CELL", "test")
	var (
		vtctldAddr   = flag.String("vtctld", defaultVtctld, "vtctld base address")
		cell         = flag.String("cell", defaultCell, "cell to query")
		timeout      = flag.Duration("timeout", defaultTimeout, "timeout for health check")
		pollInterval = flag.Duration("poll-interval", 2*time.Second, "poll interval for health check")
	)
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if err := checkEventually(ctx, *vtctldAddr, *cell, *pollInterval); err != nil {
		assert.Unreachable("Vitess cluster health did not recover within timeout", map[string]any{"error": err.Error()})
		fmt.Printf("health check failed: %v\n", err)
		os.Exit(1)
	}
}

func checkEventually(ctx context.Context, vtctldAddr string, cell string, pollInterval time.Duration) error {
	var lastErr error
	for {
		ok, err := checkOnce(vtctldAddr, cell)
		if err != nil {
			lastErr = err
		} else if ok {
			assert.Always(true, "Vitess shard has primary and replica", map[string]any{"vtctld": vtctldAddr, "cell": cell})
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

func checkOnce(vtctldAddr string, cell string) (bool, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	listURL := fmt.Sprintf("%s/api/tablets/?cell=%s", vtctldAddr, cell)
	resp, err := client.Get(listURL)
	if err != nil {
		return false, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, nil
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	var aliases []tabletAlias
	if err := json.Unmarshal(body, &aliases); err != nil {
		return false, err
	}
	if len(aliases) == 0 {
		return false, errors.New("tablet list is empty")
	}
	counts := map[string]*healthCounts{}
	for _, alias := range aliases {
		aliasStr := fmt.Sprintf("%s-%010d", alias.Cell, alias.UID)
		tabletURL := fmt.Sprintf("%s/api/tablets/%s", vtctldAddr, aliasStr)
		tr, err := fetchTabletRecord(client, tabletURL)
		if err != nil {
			return false, err
		}
		if tr.Keyspace == "" || tr.Shard == "" {
			continue
		}
		key := tr.Keyspace + ":" + tr.Shard
		entry := counts[key]
		if entry == nil {
			entry = &healthCounts{}
			counts[key] = entry
		}
		if isPrimary(tr.Type) {
			entry.primaries++
		}
		if isReplica(tr.Type) {
			entry.replicas++
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

func fetchTabletRecord(client *http.Client, url string) (*tabletRecord, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tablet fetch failed: %s", resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var record tabletRecord
	if err := json.Unmarshal(body, &record); err != nil {
		return nil, err
	}
	return &record, nil
}

func isPrimary(t interface{}) bool {
	switch value := t.(type) {
	case float64:
		return int(value) == 1
	case string:
		return value == "PRIMARY"
	default:
		return false
	}
}

func isReplica(t interface{}) bool {
	switch value := t.(type) {
	case float64:
		return int(value) == 2
	case string:
		return value == "REPLICA"
	default:
		return false
	}
}

func getenv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
