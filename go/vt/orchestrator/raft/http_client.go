/*
   Copyright 2017 Shlomi Noach, GitHub Inc.

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

package orcraft

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/ssl"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
)

var httpClient *http.Client

func setupHttpClient() error {
	httpTimeout := time.Duration(config.ActiveNodeExpireSeconds) * time.Second
	dialTimeout := func(network, addr string) (net.Conn, error) {
		return net.DialTimeout(network, addr, httpTimeout)
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.Config.SSLSkipVerify,
	}
	if config.Config.UseSSL {
		caPool, err := ssl.ReadCAFile(config.Config.SSLCAFile)
		if err != nil {
			return err
		}
		tlsConfig.RootCAs = caPool

		if config.Config.UseMutualTLS {
			var sslPEMPassword []byte
			if ssl.IsEncryptedPEM(config.Config.SSLPrivateKeyFile) {
				sslPEMPassword = ssl.GetPEMPassword(config.Config.SSLPrivateKeyFile)
			}
			if err := ssl.AppendKeyPairWithPassword(tlsConfig, config.Config.SSLCertFile, config.Config.SSLPrivateKeyFile, sslPEMPassword); err != nil {
				return err
			}
		}
	}

	httpTransport := &http.Transport{
		TLSClientConfig:       tlsConfig,
		Dial:                  dialTimeout,
		ResponseHeaderTimeout: httpTimeout,
	}
	httpClient = &http.Client{Transport: httpTransport}

	return nil
}

func HttpGetLeader(path string) (response []byte, err error) {
	leaderURI := LeaderURI.Get()
	if leaderURI == "" {
		return nil, fmt.Errorf("Raft leader URI unknown")
	}
	leaderAPI := leaderURI
	if config.Config.URLPrefix != "" {
		// We know URLPrefix begind with "/"
		leaderAPI = fmt.Sprintf("%s%s", leaderAPI, config.Config.URLPrefix)
	}
	leaderAPI = fmt.Sprintf("%s/api", leaderAPI)

	url := fmt.Sprintf("%s/%s", leaderAPI, path)

	req, err := http.NewRequest("GET", url, nil)
	switch strings.ToLower(config.Config.AuthenticationMethod) {
	case "basic", "multi":
		req.SetBasicAuth(config.Config.HTTPAuthUser, config.Config.HTTPAuthPassword)
	}

	res, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		return body, log.Errorf("HttpGetLeader: got %d status on %s", res.StatusCode, url)
	}

	return body, nil
}
