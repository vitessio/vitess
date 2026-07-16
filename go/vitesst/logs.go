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
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

type (
	// fileLogConsumer streams container log lines to a file as they arrive.
	fileLogConsumer struct {
		mu sync.Mutex
		f  *os.File
	}
)

var _ testcontainers.LogConsumer = (*fileLogConsumer)(nil)

// Accept implements testcontainers.LogConsumer.
func (fc *fileLogConsumer) Accept(l testcontainers.Log) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if fc.f == nil {
		return
	}
	if _, err := fc.f.Write(l.Content); err != nil {
		fc.f.Close()
		fc.f = nil
	}
}

// close flushes and closes the consumer's file. Later lines are dropped.
func (fc *fileLogConsumer) close() {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if fc.f != nil {
		fc.f.Close()
		fc.f = nil
	}
}

// newFileLogConsumer returns a log consumer appending to the component's log
// file, closing it when the test ends.
func (c *Cluster) newFileLogConsumer(t testing.TB, name string) *fileLogConsumer {
	f, err := os.OpenFile(filepath.Join(c.artifactDir, name+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		c.logf("opening artifact log for %s: %v", name, err)
	}

	fc := &fileLogConsumer{f: f}
	t.Cleanup(fc.close)
	return fc
}
