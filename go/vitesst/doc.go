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

// Package vitesst provides a testcontainers-based API for spinning up Vitess
// clusters in end-to-end tests. Each Vitess component runs in its own
// container on a per-cluster Docker network, so tests are parallel-safe and
// clean up after themselves even when they fail.
//
// The containers run the prebuilt vitesst:mysql80, vitesst:mysql84 and
// vitesst:mariadb Docker images. Build them from the current source tree with
// `make vitesst_images` before running tests so the containers use the code
// under test. The VITESST_IMAGE environment variable overrides the image
// entirely, and a keyspace's tablets take another image with WithImage.
//
// Usage:
//
//	func TestSomething(t *testing.T) {
//	    c, err := vitesst.NewCluster(t,
//	        vitesst.WithKeyspace("ks").
//	            WithSchema(`CREATE TABLE users (id INT PRIMARY KEY)`).
//	            WithVSchema(`{"sharded": false, "tables": {"users": {}}}`),
//	    )
//	    require.NoError(t, err)
//	    cleanup, err := c.Start(t, t.Context())
//	    t.Cleanup(func() {
//	        if err := cleanup(context.WithoutCancel(t.Context())); err != nil {
//	            t.Logf("cluster teardown: %v", err)
//	        }
//	    })
//	    require.NoError(t, err)
//
//	    conn := c.Connect(t)
//	    defer conn.Close()
//
//	    vitesst.Exec(t, conn, "INSERT INTO ks.users (id) VALUES (1)")
//	}
//
// Every component's log lines stream to one file per component under the
// test's artifact directory. Run go test with -artifacts to retain them.
package vitesst
