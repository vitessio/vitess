package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
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

	if err := waitForHealth(ctx, vtorcAddr+"/debug/health"); err != nil {
		fmt.Printf("vtorc health check failed: %v\n", err)
		os.Exit(1)
	}
	if err := waitForHealth(ctx, vtgateAddr+"/debug/health"); err != nil {
		fmt.Printf("vtgate health check failed: %v\n", err)
		os.Exit(1)
	}

	if err := waitForTabletTopology(ctx, vtctldAddr, vtctldCell); err != nil {
		fmt.Printf("vtctld tablet topology unavailable: %v\n", err)
		os.Exit(1)
	}

	lifecycle.SetupComplete(map[string]any{"message": "vitess cluster is healthy"})
	select {}
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
