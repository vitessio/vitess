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
)

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
	}

	if rec.HasErrors() {
		return rec.Error()
	}

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
		// TODO: maybe log a warning?
		c.authenticator = nil // Technically a no-op, but being super explicit about it.
	}

	c.reified = true
	return nil
}

func (c *Config) GetAuthenticator() Authenticator {
	return c.authenticator
}

func (c *Config) GetAuthorizer() *Authorizer {
	return c.authorizer
}
