/*
Copyright 2021 The Vitess Authors.

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

package rbac

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
	"k8s.io/apimachinery/pkg/util/sets"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/log"
)

// Config is the RBAC configuration representation. The public fields are
// populated by viper during LoadConfig, and the private fields are set during
// cfg.Reify. A config must be reified before first use.
type Config struct {
	Authenticator string
	Rules         []*struct {
		Resource string
		Actions  []string
		Subjects []string
		Clusters []string
	}

	reified bool

	cfg           map[string][]*Rule
	authenticator Authenticator
	authorizer    *Authorizer
}

// LoadConfig reads the file at path into a Config struct, and then reifies
// the config so its autheticator and authorizer may be used. Errors during
// loading/parsing, or validation errors during reification are returned to the
// caller.
//
// Any file format supported by viper is supported. Currently this is yaml, json
// or toml.
func LoadConfig(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	var cfg Config
	if err := v.UnmarshalExact(&cfg); err != nil {
		return nil, err
	}

	if err := cfg.Reify(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// Reify makes a config that was loaded from a file usable, by validating the
// rules and constructing its (optional) authenticator and authorizer. A config
// must be reified before first use. Calling Reify multiple times has no effect
// after the first call. Reify is called by LoadConfig, so a config loaded that
// way does not need to be manually reified.
func (c *Config) Reify() error {
	if c.reified {
		return nil
	}

	// reify the rules
	byResource := map[string][]*Rule{}
	rec := concurrency.AllErrorRecorder{}

	for i, rule := range c.Rules {
		resourceRules := byResource[rule.Resource]

		actions := sets.NewString(rule.Actions...)
		if actions.Has("*") && actions.Len() > 1 {
			// error to have wildcard and something else
			rec.RecordError(fmt.Errorf("rule %d: actions list cannot include wildcard and other actions, have %v", i, actions.List()))
		}

		subjects := sets.NewString(rule.Subjects...)
		if subjects.Has("*") && subjects.Len() > 1 {
			// error to have wildcard and something else
			rec.RecordError(fmt.Errorf("rule %d: subjects list cannot include wildcard and other subjects, have %v", i, subjects.List()))
		}

		clusters := sets.NewString(rule.Clusters...)
		if clusters.Has("*") && clusters.Len() > 1 {
			// error to have wildcard and something else
			rec.RecordError(fmt.Errorf("rule %d: clusters list cannot include wildcard and other clusters, have %v", i, clusters.List()))
		}

		resourceRules = append(resourceRules, &Rule{
			actions:  actions,
			subjects: subjects,
			clusters: clusters,
		})
		byResource[rule.Resource] = resourceRules
	}

	if rec.HasErrors() {
		return rec.Error()
	}

	log.Infof("[rbac]: loaded authorizer with %d rules", len(c.Rules))

	c.cfg = byResource
	c.authorizer = &Authorizer{
		policies: c.cfg,
	}

	// reify the authenticator
	switch {
	case strings.HasSuffix(c.Authenticator, ".so"):
		authn, err := loadAuthenticatorPlugin(c.Authenticator)
		if err != nil {
			return err
		}

		c.authenticator = authn
	case c.Authenticator != "":
		factory, ok := authenticators[c.Authenticator]
		if !ok {
			return fmt.Errorf("%w %s", ErrUnregisteredAuthenticationImpl, c.Authenticator)
		}

		c.authenticator = factory()
	default:
		log.Info("[rbac]: no authenticator implementation specified")
		c.authenticator = nil // Technically a no-op, but being super explicit about it.
	}

	c.reified = true
	return nil
}

// GetAuthenticator returns the Authenticator implementation specified by the
// config. It returns nil if the Authenticator string field is the empty string,
// or if a call to Reify has not been made.
func (c *Config) GetAuthenticator() Authenticator {
	return c.authenticator
}

// GetAuthorizer returns the Authorizer using the rules specified in the config.
// It returns nil if a call to Reify has not been made.
func (c *Config) GetAuthorizer() *Authorizer {
	return c.authorizer
}

// DefaultConfig returns a default config that allows all actions on all resources
// It is mainly used in the case where users explicitly pass --no-rbac flag.
func DefaultConfig() *Config {
	log.Info("[rbac]: using default rbac configuration")
	actions := []string{string(GetAction), string(CreateAction), string(DeleteAction), string(PutAction), string(PingAction)}
	subjects := []string{"*"}
	clusters := []string{"*"}

	cfg := map[string][]*Rule{
		"*": {
			{
				clusters: sets.NewString(clusters...),
				actions:  sets.NewString(actions...),
				subjects: sets.NewString(subjects...),
			},
		},
	}

	return &Config{
		Rules: []*struct {
			Resource string
			Actions  []string
			Subjects []string
			Clusters []string
		}{
			{
				Resource: "*",
				Actions:  actions,
				Subjects: subjects,
				Clusters: clusters,
			},
		},
		cfg: cfg,
		authorizer: &Authorizer{
			policies: cfg,
		},
		authenticator: nil,
	}
}
