/*
Copyright 2020 The Vitess Authors.

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

// Package fakevtsql provides an interface for mocking out sql.DB responses in
// tests that depend on a vtsql.DB instance.
//
// To use fakevtsql, you will need to create a discovery implementation that
// does not error, e.g. with fakediscovery:
//
//		disco := fakediscovery.New()
//		disco.AddTaggedGates(nil, []*vtadminpb.VTGate{Hostname: "gate"})
//
// Then, you will call vtsql.New(), passing the faked discovery implementation
// into the config:
//
//		db := vtsql.New(&vtsql.Config{
//          Cluster: &vtadminpb.Cluster{Id: "cid", Name: "cluster"},
//			Discovery: disco,
//		})
//
// Finally, with your instantiated VTGateProxy instance, you can mock out the
// DialFunc to always return a fakevtsql.Connector. The Tablets and ShouldErr
// attributes of the connector control the behavior:
//
//		db.DialFunc = func(cfg vitessdriver.Configuration) (*sql.DB, error) {
//			return sql.OpenDB(&fakevtsql.Connector{
//				Tablets: mockTablets,
//				ShouldErr: shouldErr,
//			})
//		}
//		cluster := &cluster.Cluster{
//			/* other attributes */
//			DB: db,
//		}
//
// go/vt/vtadmin/api_test.go has several examples of usage.
package fakevtsql
