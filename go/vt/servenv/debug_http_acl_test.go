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

package servenv

import (
	"io"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestUnauthenticatedProfileEndpointsDoNotChangeRates checks the debug
// profile handlers registered on the default servenv mux. An anonymous POST
// must not change the block or mutex profile rate.
func TestUnauthenticatedProfileEndpointsDoNotChangeRates(t *testing.T) {
	beforeBlock := blockProfileRate
	beforeMutex := runtime.SetMutexProfileFraction(-1)
	t.Cleanup(func() {
		blockProfileRate = beforeBlock
		runtime.SetBlockProfileRate(beforeBlock)
		runtime.SetMutexProfileFraction(beforeMutex)
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	go func() { _ = HTTPServe(ln) }()

	base := "http://" + ln.Addr().String()
	require.Eventually(t, func() bool {
		resp, err := http.Get(base + "/debug/liveness")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 10*time.Millisecond)

	blockBody, blockCode := postForm(t, base+"/debug/blockprofilerate", url.Values{"rate": {"1"}})
	require.Equal(t, beforeBlock, blockProfileRate)
	require.Equalf(t, http.StatusForbidden, blockCode, "anonymous /debug/blockprofilerate body: %s", blockBody)

	mutexBody, mutexCode := postForm(t, base+"/debug/mutexprofilefraction", url.Values{"fraction": {"1"}})
	require.Equal(t, beforeMutex, runtime.SetMutexProfileFraction(-1))
	require.Equalf(t, http.StatusForbidden, mutexCode, "anonymous /debug/mutexprofilefraction body: %s", mutexBody)
}

func postForm(t *testing.T, rawURL string, form url.Values) (string, int) {
	t.Helper()
	resp, err := http.PostForm(rawURL, form)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	require.NoError(t, err)
	return string(body), resp.StatusCode
}
