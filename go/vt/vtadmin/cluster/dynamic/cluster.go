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

package dynamic

import (
	"encoding/base64"
	"errors"
	"maps"
	"slices"
	"strings"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vtadmin/cluster"
)

// ConfigFromString returns a cluster ID and possibly a cluster Config from a
// base64-encoded JSON spec. It does not build the cluster, so it reads no
// files and dials no addresses.
//
// If an error occurs decoding or unmarshalling the string, this function
// returns, respectively, a nil Config, the empty string, and a non-nil error.
//
// If the string can be decoded and unmarshalled, then the id will be non-empty,
// but the Config is nil and the error non-nil if the spec is invalid or sets an
// option that a client may not set.
//
// Therefore, callers should handle the return values as follows:
//
//	cfg, id, err := dynamic.ConfigFromString(s)
//	if id == "" {
//		// handle err, do not use `cfg`.
//	}
//	if err != nil {
//		// log error. `cfg` is nil, so only an existing cluster with ID `id` can be used.
//	}
//	dynamicAPI, err := api.WithCluster(ctx, id, cfg, s)
//	if err != nil {
//		// neither `cfg` nor an existing cluster with ID: `id` is available.
//	}
//	dynamicAPI.DoAThing()
func ConfigFromString(s string) (cfg *cluster.Config, id string, err error) {
	cfg, id, err = cluster.LoadConfig(base64.NewDecoder(base64.StdEncoding, strings.NewReader(s)), "json")
	if err != nil {
		return nil, id, err
	}

	if err := checkClientConfig(cfg); err != nil {
		return nil, id, err
	}

	return cfg, id, nil
}

// checkClientConfig refuses a client-supplied spec that would make VTAdmin
// read a local file or send its own credentials to the spec's addresses.
func checkClientConfig(cfg *cluster.Config) error {
	// Each flag is passed on as "--key=value", so a key containing "=" could
	// set a flag whose name differs from the key checked below.
	flagSets := append([]map[string]string{cfg.VtSQLFlags, cfg.VtctldFlags}, slices.Collect(maps.Values(cfg.DiscoveryFlagsByImpl))...)
	for _, flags := range flagSets {
		for key := range flags {
			if strings.Contains(key, "=") {
				return errors.New("a dynamic cluster spec cannot have a flag name containing '='")
			}
		}
	}

	if _, ok := cfg.VtSQLFlags["credentials-path-tmpl"]; ok {
		return errors.New("a dynamic cluster spec cannot set vtsql-credentials-path-tmpl")
	}

	if _, ok := cfg.VtctldFlags["credentials-path-tmpl"]; ok {
		return errors.New("a dynamic cluster spec cannot set vtctld-credentials-path-tmpl")
	}

	if cfg.DiscoveryImpl == "staticfile" {
		return errors.New("a dynamic cluster spec cannot use staticfile discovery")
	}

	if grpcclient.StaticAuthConfigured() {
		return errors.New("dynamic clusters cannot be used with --grpc-auth-static-client-creds")
	}

	return nil
}
