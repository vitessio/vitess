/*
Copyright 2026 The Vitess Authors.

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

package vitesst

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

type (
	// component is the shared core of every Vitess container the framework
	// manages: a name (also the container's network alias), the container
	// handle, and HTTP observability helpers. The handle is mutex-guarded
	// because components like vtgate can be recreated behind a stable handle.
	component struct {
		name     string
		httpPort string // container port, e.g. "15001/tcp"
		cluster  *Cluster

		mu  sync.Mutex
		ctr testcontainers.Container
	}
)

// Name returns the component name, which is also its network alias.
func (cp *component) Name() string {
	return cp.name
}

// container returns the current container handle.
func (cp *component) container() testcontainers.Container {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return cp.ctr
}

// setContainer swaps the container handle, returning the previous one.
func (cp *component) setContainer(ctr testcontainers.Container) testcontainers.Container {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	old := cp.ctr
	cp.ctr = ctr
	return old
}

// HTTPAddr returns the host-reachable "host:port" of the component's HTTP
// endpoint. Mapped ports change when a container restarts, so the address is
// re-resolved on every call.
func (cp *component) HTTPAddr(ctx context.Context) (string, error) {
	return cp.hostAddr(ctx, cp.httpPort)
}

// hostAddr resolves a container port to a host-reachable "host:port".
func (cp *component) hostAddr(ctx context.Context, port string) (string, error) {
	ctr := cp.container()
	if ctr == nil {
		return "", fmt.Errorf("%s has no container", cp.name)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("resolving %s host: %w", cp.name, err)
	}

	mapped, err := ctr.MappedPort(ctx, port)
	if err != nil {
		return "", fmt.Errorf("resolving %s mapped port %s: %w", cp.name, port, err)
	}

	return fmt.Sprintf("%s:%d", host, mapped.Num()), nil
}

// MakeAPICall performs a GET against a path on the component's HTTP port and
// returns the status code and response body.
func (cp *component) MakeAPICall(ctx context.Context, path string) (int, string, error) {
	addr, err := cp.HTTPAddr(ctx)
	if err != nil {
		return 0, "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		return 0, "", fmt.Errorf("building request for %s%s: %w", cp.name, path, err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("calling %s%s: %w", cp.name, path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, "", fmt.Errorf("reading %s%s response: %w", cp.name, path, err)
	}

	return resp.StatusCode, string(body), nil
}

// MakeAPICallRetry polls a path on the component's HTTP port until the given
// predicate accepts the status code and body, or the timeout elapses.
func (cp *component) MakeAPICallRetry(ctx context.Context, path string, timeout time.Duration, accept func(status int, body string) bool) (int, string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var (
		status  int
		body    string
		lastErr error
	)
	for {
		status, body, lastErr = cp.MakeAPICall(ctx, path)
		if lastErr == nil && accept(status, body) {
			return status, body, nil
		}

		select {
		case <-ctx.Done():
			return status, body, fmt.Errorf("%s%s did not reach the expected state within %s: %w", cp.name, path, timeout, errFirst(lastErr, ctx.Err()))
		case <-time.After(defaultPollInterval):
		}
	}
}

// GetVars fetches and decodes the component's /debug/vars.
func (cp *component) GetVars(ctx context.Context) (map[string]any, error) {
	status, body, err := cp.MakeAPICall(ctx, "/debug/vars")
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("%s /debug/vars returned status %d", cp.name, status)
	}

	vars := make(map[string]any)
	if err := json.Unmarshal([]byte(body), &vars); err != nil {
		return nil, fmt.Errorf("decoding %s /debug/vars: %w", cp.name, err)
	}
	return vars, nil
}

// GetMetrics fetches the component's Prometheus /metrics endpoint.
func (cp *component) GetMetrics(ctx context.Context) (string, error) {
	status, body, err := cp.MakeAPICall(ctx, "/metrics")
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("%s /metrics returned status %d", cp.name, status)
	}
	return body, nil
}

// Logs returns the component's full container log from the Docker daemon,
// stdout and stderr interleaved, oldest first.
func (cp *component) Logs(ctx context.Context) (string, error) {
	ctr := cp.container()
	if ctr == nil {
		return "", fmt.Errorf("%s has no container", cp.name)
	}

	rc, err := ctr.Logs(ctx)
	if err != nil {
		return "", fmt.Errorf("reading %s logs: %w", cp.name, err)
	}
	defer rc.Close()

	content, err := io.ReadAll(rc)
	if err != nil {
		return "", fmt.Errorf("reading %s logs: %w", cp.name, err)
	}
	return string(content), nil
}

// Exec runs a command inside the component's container and returns its exit
// code and combined output. It blocks until the command exits.
func (cp *component) Exec(ctx context.Context, cmd ...string) (int, string, error) {
	ctr := cp.container()
	if ctr == nil {
		return 0, "", fmt.Errorf("%s has no container", cp.name)
	}
	return containerExec(ctx, ctr, cmd)
}

// ReadFile returns the contents of a file inside the component's container.
func (cp *component) ReadFile(ctx context.Context, path string) (string, error) {
	exitCode, output, err := cp.Exec(ctx, "cat", path)
	if err != nil {
		return "", err
	}
	if exitCode != 0 {
		return "", fmt.Errorf("reading %s on %s: %s", path, cp.name, output)
	}
	return output, nil
}

// WriteFile writes content to a path inside the component's container,
// including paths on tmpfs mounts.
func (cp *component) WriteFile(ctx context.Context, path, content string) error {
	ctr := cp.container()
	if ctr == nil {
		return fmt.Errorf("%s has no container", cp.name)
	}
	if err := writeContainerFile(ctx, ctr, path, content); err != nil {
		return fmt.Errorf("writing %s on %s: %w", path, cp.name, err)
	}
	return nil
}

// RemoveFile deletes a path inside the component's container, recursively when
// it is a directory.
func (cp *component) RemoveFile(ctx context.Context, path string) error {
	if _, _, err := cp.Exec(ctx, "rm", "-rf", path); err != nil {
		return fmt.Errorf("removing %s on %s: %w", path, cp.name, err)
	}
	return nil
}

// StopContainer stops the component's container gracefully with SIGTERM,
// killing it after the timeout.
func (cp *component) StopContainer(ctx context.Context, timeout time.Duration) error {
	ctr := cp.container()
	if ctr == nil {
		return fmt.Errorf("%s has no container", cp.name)
	}
	return ctr.Stop(ctx, &timeout)
}

// StartContainer starts a stopped container and blocks until its readiness
// wait passes again.
func (cp *component) StartContainer(ctx context.Context) error {
	ctr := cp.container()
	if ctr == nil {
		return fmt.Errorf("%s has no container", cp.name)
	}
	return ctr.Start(ctx)
}

// IsRunning reports whether the component's container is running.
func (cp *component) IsRunning() bool {
	ctr := cp.container()
	return ctr != nil && ctr.IsRunning()
}

// terminate tears the component's container down immediately.
func (cp *component) terminate(ctx context.Context) error {
	ctr := cp.setContainer(nil)
	if ctr == nil {
		return nil
	}
	return testcontainers.TerminateContainer(ctr, testcontainers.StopContext(ctx), testcontainers.StopTimeout(0))
}

// errFirst returns the first non-nil error.
func errFirst(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
