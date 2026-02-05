/*
Copyright 2025 The Vitess Authors.

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

package querythrottler

import "context"

// fakeConfigLoader is a test fake that implements ConfigLoader.
type fakeConfigLoader struct {
	giveConfig Config
}

// newFakeConfigLoader creates a fake config loader
// with a fully constructed Config.
func newFakeConfigLoader(cfg Config) *fakeConfigLoader {
	return &fakeConfigLoader{
		giveConfig: cfg,
	}
}

// Load implements the ConfigLoader interface.
func (f *fakeConfigLoader) Load(ctx context.Context) (Config, error) {
	return f.giveConfig, nil
}
