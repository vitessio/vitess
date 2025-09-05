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
