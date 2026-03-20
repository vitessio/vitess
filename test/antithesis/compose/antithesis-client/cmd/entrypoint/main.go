package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
)

type (
	healthCounts struct {
		primaries int
		replicas  int
	}

	tabletTarget struct {
		TabletType int `json:"tablet_type"`
	}

	tabletInfo struct {
		Keyspace string `json:"keyspace"`
		Shard    string `json:"shard"`
	}

	tabletStats struct {
		Tablet    tabletInfo   `json:"Tablet"`
		Target    tabletTarget `json:"Target"`
		Serving   bool         `json:"Serving"`
		LastError *string      `json:"LastError"`
	}

	tabletCacheStatus struct {
		TabletsStats []tabletStats `json:"TabletsStats"`
	}
)

const (
	defaultPollInterval = 5 * time.Second
	defaultTimeout      = 10 * time.Minute
)

func main() {
	vtorcAddr := getenv("VTORC_ADDR", "http://vtorc:8080")
	vtgateAddr := getenv("VTGATE_ADDR", "http://vtgate:8080")
	vtctldAddr := getenv("VTCTLD_ADDR", "http://vtctld:8080")
	vtctldCell := getenv("VTCTLD_CELL", "test")
	ctx, cancel := context.WithTimeout(context.Background(), envDuration("SETUP_TIMEOUT", defaultTimeout))
	defer cancel()

	fmt.Println("waiting for vtorc health...")
	if err := waitForHealth(ctx, vtorcAddr+"/debug/health"); err != nil {
		fmt.Printf("vtorc health check failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("vtorc is healthy")

	fmt.Println("waiting for vtgate health...")
	if err := waitForHealth(ctx, vtgateAddr+"/debug/health"); err != nil {
		fmt.Printf("vtgate health check failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("vtgate is healthy")

	fmt.Println("waiting for tablet topology...")
	if err := waitForTabletTopology(ctx, vtctldAddr, vtctldCell); err != nil {
		fmt.Printf("vtctld tablet topology unavailable: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("tablet topology ready")

	fmt.Println("waiting for healthy primaries and replicas on all shards...")
	if err := waitForHealthyShards(ctx, vtgateAddr, vtctldCell); err != nil {
		fmt.Printf("shard health check failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("all shards have healthy primaries and replicas")

	lifecycle.SetupComplete(map[string]any{"message": "vitess cluster is healthy"})
	fmt.Println("setup complete, cluster is healthy")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("signal received, shutting down...")
}

func waitForHealth(ctx context.Context, url string) error {
	client := &http.Client{Timeout: 5 * time.Second}
	return poll(ctx, envDuration("SETUP_POLL_INTERVAL", defaultPollInterval), func() (bool, error) {
		resp, err := client.Get(url)
		if err != nil {
			return false, nil
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK, nil
	})
}

func waitForTabletTopology(ctx context.Context, vtctldAddr string, cell string) error {
	client := &http.Client{Timeout: 5 * time.Second}
	url := fmt.Sprintf("%s/api/tablets/?cell=%s", vtctldAddr, cell)
	return poll(ctx, envDuration("SETUP_POLL_INTERVAL", defaultPollInterval), func() (bool, error) {
		resp, err := client.Get(url)
		if err != nil {
			return false, nil
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false, nil
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return false, nil
		}
		var payload []any
		if err := json.Unmarshal(body, &payload); err != nil {
			return false, nil
		}
		return len(payload) > 0, nil
	})
}

func waitForHealthyShards(ctx context.Context, vtgateAddr string, cell string) error {
	client := &http.Client{Timeout: 5 * time.Second}
	url := fmt.Sprintf("%s/api/health-check/cell/%s", vtgateAddr, cell)
	return poll(ctx, envDuration("SETUP_POLL_INTERVAL", defaultPollInterval), func() (bool, error) {
		resp, err := client.Get(url)
		if err != nil {
			return false, nil
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return false, nil
		}
		var cacheStatuses []tabletCacheStatus
		if err := json.NewDecoder(resp.Body).Decode(&cacheStatuses); err != nil {
			return false, nil
		}
		if len(cacheStatuses) == 0 {
			return false, nil
		}
		counts := map[string]*healthCounts{}
		for _, cs := range cacheStatuses {
			for _, tablet := range cs.TabletsStats {
				if tablet.Tablet.Keyspace == "" || tablet.Tablet.Shard == "" {
					continue
				}
				if !isTabletHealthy(tablet) {
					continue
				}
				key := tablet.Tablet.Keyspace + ":" + tablet.Tablet.Shard
				entry := counts[key]
				if entry == nil {
					entry = &healthCounts{}
					counts[key] = entry
				}
				if tablet.Target.TabletType == 1 {
					entry.primaries++
				}
				if tablet.Target.TabletType == 2 {
					entry.replicas++
				}
			}
		}
		if len(counts) == 0 {
			return false, nil
		}
		for _, entry := range counts {
			// Require 1 primary and 2 replicas for setup complete 
			if entry.primaries < 1 || entry.replicas < 2 {
				return false, nil
			}
		}
		return true, nil
	})
}

func isTabletHealthy(s tabletStats) bool {
	if !s.Serving {
		return false
	}
	if s.LastError == nil {
		return true
	}
	return *s.LastError == ""
}

func poll(ctx context.Context, interval time.Duration, fn func() (bool, error)) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		ok, err := fn()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
