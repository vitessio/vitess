/*
   Copyright 2014 Outbrain Inc.

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

package app

import (
	"flag"
	"net"
	nethttp "net/http"
	"path"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/orchestrator/collection"
	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/http"
	"vitess.io/vitess/go/vt/orchestrator/inst"
	"vitess.io/vitess/go/vt/orchestrator/logic"
	"vitess.io/vitess/go/vt/orchestrator/process"
	"vitess.io/vitess/go/vt/orchestrator/ssl"

	"github.com/go-martini/martini"
	"github.com/martini-contrib/auth"
	"github.com/martini-contrib/gzip"
	"github.com/martini-contrib/render"
)

const discoveryMetricsName = "DISCOVERY_METRICS"

// TODO(sougou): see if this can be embedded.
var webDir = flag.String("orc_web_dir", "web/orchestrator", "Orchestrator http file location")

var sslPEMPassword []byte
var agentSSLPEMPassword []byte
var discoveryMetrics *collection.Collection

// HTTP starts serving
func HTTP(continuousDiscovery bool) {
	promptForSSLPasswords()
	process.ContinuousRegistration(string(process.OrchestratorExecutionHTTPMode), "")

	martini.Env = martini.Prod
	standardHTTP(continuousDiscovery)
}

// Iterate over the private keys and get passwords for them
// Don't prompt for a password a second time if the files are the same
func promptForSSLPasswords() {
	if ssl.IsEncryptedPEM(config.Config.SSLPrivateKeyFile) {
		sslPEMPassword = ssl.GetPEMPassword(config.Config.SSLPrivateKeyFile)
	}
	if ssl.IsEncryptedPEM(config.Config.AgentSSLPrivateKeyFile) {
		if config.Config.AgentSSLPrivateKeyFile == config.Config.SSLPrivateKeyFile {
			agentSSLPEMPassword = sslPEMPassword
		} else {
			agentSSLPEMPassword = ssl.GetPEMPassword(config.Config.AgentSSLPrivateKeyFile)
		}
	}
}

// standardHTTP starts serving HTTP or HTTPS (api/web) requests, to be used by normal clients
func standardHTTP(continuousDiscovery bool) {
	m := martini.Classic()

	switch strings.ToLower(config.Config.AuthenticationMethod) {
	case "basic":
		{
			if config.Config.HTTPAuthUser == "" {
				// Still allowed; may be disallowed in future versions
				log.Warning("AuthenticationMethod is configured as 'basic' but HTTPAuthUser undefined. Running without authentication.")
			}
			m.Use(auth.Basic(config.Config.HTTPAuthUser, config.Config.HTTPAuthPassword))
		}
	case "multi":
		{
			if config.Config.HTTPAuthUser == "" {
				// Still allowed; may be disallowed in future versions
				log.Fatal("AuthenticationMethod is configured as 'multi' but HTTPAuthUser undefined")
			}

			m.Use(auth.BasicFunc(func(username, password string) bool {
				if username == "readonly" {
					// Will be treated as "read-only"
					return true
				}
				return auth.SecureCompare(username, config.Config.HTTPAuthUser) && auth.SecureCompare(password, config.Config.HTTPAuthPassword)
			}))
		}
	default:
		{
			// We inject a dummy User object because we have function signatures with User argument in api.go
			m.Map(auth.User(""))
		}
	}

	m.Use(gzip.All())
	// Render html templates from templates directory
	m.Use(render.Renderer(render.Options{
		Directory:       *webDir,
		Layout:          "templates/layout",
		HTMLContentType: "text/html",
	}))
	m.Use(martini.Static(path.Join(*webDir, "public"), martini.StaticOptions{Prefix: config.Config.URLPrefix}))
	if config.Config.UseMutualTLS {
		m.Use(ssl.VerifyOUs(config.Config.SSLValidOUs))
	}

	inst.SetMaintenanceOwner(process.ThisHostname)

	if continuousDiscovery {
		// start to expire metric collection info
		discoveryMetrics = collection.CreateOrReturnCollection(discoveryMetricsName)
		discoveryMetrics.SetExpirePeriod(time.Duration(config.Config.DiscoveryCollectionRetentionSeconds) * time.Second)

		log.Info("Starting Discovery")
		go logic.ContinuousDiscovery()
	}

	log.Info("Registering endpoints")
	http.HTTPapi.URLPrefix = config.Config.URLPrefix
	http.HTTPWeb.URLPrefix = config.Config.URLPrefix
	http.HTTPapi.RegisterRequests(m)
	http.HTTPWeb.RegisterRequests(m)

	// Serve
	if config.Config.ListenSocket != "" {
		log.Infof("Starting HTTP listener on unix socket %v", config.Config.ListenSocket)
		unixListener, err := net.Listen("unix", config.Config.ListenSocket)
		if err != nil {
			log.Fatal(err)
		}
		defer unixListener.Close()
		if err := nethttp.Serve(unixListener, m); err != nil {
			log.Fatal(err)
		}
	} else if config.Config.UseSSL {
		log.Info("Starting HTTPS listener")
		tlsConfig, err := ssl.NewTLSConfig(config.Config.SSLCAFile, config.Config.UseMutualTLS)
		if err != nil {
			log.Fatal(err)
		}
		tlsConfig.InsecureSkipVerify = config.Config.SSLSkipVerify
		if err = ssl.AppendKeyPairWithPassword(tlsConfig, config.Config.SSLCertFile, config.Config.SSLPrivateKeyFile, sslPEMPassword); err != nil {
			log.Fatal(err)
		}
		if err = ssl.ListenAndServeTLS(config.Config.ListenAddress, m, tlsConfig); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Infof("Starting HTTP listener on %+v", config.Config.ListenAddress)
		if err := nethttp.ListenAndServe(config.Config.ListenAddress, m); err != nil {
			log.Fatal(err)
		}
	}
	log.Info("Web server started")
}
