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
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttls"
)

// clientTLSReloadName is the label of the metrics of the reloads of
// the process's clients' TLS files.
const clientTLSReloadName = "clients"

// tlsReloadBinaries are the long-running binaries that reload their
// TLS files, and so have --tls-reload-interval.
var tlsReloadBinaries = []string{
	"mysqlctld",
	"vtbackup",
	"vtcombo",
	"vtctld",
	"vtgate",
	"vtgateclienttest",
	"vtorc",
	"vttablet",
	"vttestserver",
}

func init() {
	for _, cmd := range tlsReloadBinaries {
		OnParseFor(cmd, registerTLSReloadFlags)
	}
}

// tlsReloadFlagsRegistered is set in the binaries that reload their TLS
// files, see tlsReloadBinaries.
var tlsReloadFlagsRegistered bool

func registerTLSReloadFlags(fs *pflag.FlagSet) {
	tlsReloadFlagsRegistered = true
	utils.SetFlagDurationVar(fs, &tlsReloadInterval, "tls-reload-interval", tlsReloadInterval, "how often to check the TLS certificate, key, CA and CRL files of the process's gRPC and MySQL servers and clients for changes and reload them, 0 to disable; SIGHUP always reloads them")
}

// startClientTLSReload reloads the files of the process's TLS clients
// on every SIGHUP, and every interval if it is positive, until the
// process terminates. It does nothing in a binary that does not reload
// its TLS files, which keeps its own handling of SIGHUP.
func startClientTLSReload(interval time.Duration) {
	if !tlsReloadFlagsRegistered {
		return
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer signal.Stop(signals)
		runClientTLSReload(ctx, signals, interval)
	}()
	OnTermSync(func() {
		cancel()
		<-done
	})
}

// runClientTLSReload reloads the files of the process's TLS clients on
// every signal received on signals, and every interval if it is
// positive, until ctx is done.
func runClientTLSReload(ctx context.Context, signals <-chan os.Signal, interval time.Duration) {
	var tick <-chan time.Time
	if interval > 0 {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		tick = ticker.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-signals:
		case <-tick:
		}
		reloadClientTLSFiles()
	}
}

// reloadClientTLSFiles reloads the files of the process's TLS clients.
// New connections use them; established ones keep what they negotiated.
func reloadClientTLSFiles() {
	changed, err := vttls.ReloadCachedFiles()
	if err != nil {
		tlsReloadErrors.Add(clientTLSReloadName, 1)
		log.Error(vterrors.Wrapf(err, "cannot reload some of the TLS files of the process's clients; each of those keeps what was loaded from it before").Error())
	} else {
		tlsReloadSuccessTimestamp.Set(clientTLSReloadName, time.Now().Unix())
	}
	if changed {
		log.Info("Reloaded the TLS files of the process's clients")
	}
}
