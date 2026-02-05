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

package registry

import (
	"fmt"
	"sync"

	"vitess.io/vitess/go/vt/log"
)

var (
	mu        sync.RWMutex
	factories = map[ThrottlingStrategy]StrategyFactory{}
)

// Register registers a new strategy factory with the given name.
// Panics if a strategy with the same name is already registered (fail-fast behavior).
func Register(name ThrottlingStrategy, factory StrategyFactory) {
	mu.Lock()
	defer mu.Unlock()

	if _, exists := factories[name]; exists {
		panic(fmt.Sprintf("strategy %s already registered", name))
	}

	factories[name] = factory
	log.Infof("Registered throttling strategy: %s", name)
}

// Get retrieves a strategy factory by name.
// Returns the factory and true if found, nil and false otherwise.
func Get(name ThrottlingStrategy) (StrategyFactory, bool) {
	mu.RLock()
	defer mu.RUnlock()

	factory, exists := factories[name]
	return factory, exists
}

// CreateStrategy creates a new strategy instance using the registered factory.
// Falls back to NoOpStrategy for unknown strategies or factory errors.
func CreateStrategy(cfg StrategyConfig, deps Deps) ThrottlingStrategyHandler {
	// If the requested strategy name is unknown or the factory panics/returns error, CreateStrategy() directly constructs &NoOpStrategy{} as its unconditional fallback.
	// Configuration files or runtime switches never list "NoOp" as a valid strategy choice; they list “TabletThrottler”, “Cinnamon”, etc.
	// The design intent is:
	// Every “real” strategy must self-register to opt-in.
	// NoOpStrategy must always be available—even before any registration happens—so the registry itself can safely fall back on it.
	factory, ok := Get(cfg.GetStrategy())
	if !ok {
		log.Warningf("Unknown strategy %s, using NoOp", cfg.GetStrategy())
		return &NoOpStrategy{}
	}

	strategy, err := factory.New(deps, cfg)
	if err != nil {
		log.Errorf("Strategy %s failed to init: %v, using NoOp", cfg.GetStrategy(), err)
		return &NoOpStrategy{}
	}

	return strategy
}

// ListRegistered returns a list of all registered strategy names.
// Useful for debugging and testing.
func ListRegistered() []ThrottlingStrategy {
	mu.RLock()
	defer mu.RUnlock()

	strategies := make([]ThrottlingStrategy, 0, len(factories))
	for name := range factories {
		strategies = append(strategies, name)
	}
	return strategies
}
