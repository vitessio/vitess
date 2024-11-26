/*
Copyright 2024 The Vitess Authors.

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

package errorsbackup

import (
	"errors"
	"strings"
	"sync"
)

type BackupErrorRecorder interface {
	RecordError(string, error)
	HasErrors() bool
	Error() error
	GetFailedFiles() []string
	ResetErrorForFile(string)
}

// PerFileErrorRecorder records all the errors.
type PerFileErrorRecorder struct {
	mu     sync.Mutex
	errors map[string][]error
}

// RecordError records a possible error:
// - does nothing if err is nil
func (pfer *PerFileErrorRecorder) RecordError(filename string, err error) {
	if err == nil {
		return
	}

	pfer.mu.Lock()
	defer pfer.mu.Unlock()

	if pfer.errors == nil {
		pfer.errors = make(map[string][]error)
	}
	pfer.errors[filename] = append(pfer.errors[filename], err)
}

// HasErrors returns true if we ever recorded an error
func (pfer *PerFileErrorRecorder) HasErrors() bool {
	pfer.mu.Lock()
	defer pfer.mu.Unlock()
	return len(pfer.errors) > 0
}

func (pfer *PerFileErrorRecorder) Error() error {
	pfer.mu.Lock()
	defer pfer.mu.Unlock()
	if pfer.errors == nil {
		return nil
	}

	var errs []string
	for _, fileErrs := range pfer.errors {
		for _, err := range fileErrs {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.New(strings.Join(errs, "; "))
}

func (pfer *PerFileErrorRecorder) GetFailedFiles() []string {
	pfer.mu.Lock()
	defer pfer.mu.Unlock()
	if pfer.errors == nil {
		return nil
	}
	var files []string
	for filename := range pfer.errors {
		files = append(files, filename)
	}
	return files
}

func (pfer *PerFileErrorRecorder) ResetErrorForFile(filename string) {
	pfer.mu.Lock()
	defer pfer.mu.Unlock()
	if pfer.errors == nil {
		return
	}
	delete(pfer.errors, filename)
}
