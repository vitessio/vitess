/*
Copyright 2019 The Vitess Authors.

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

package vtgate

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/callinfo"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/binlogacl"
	"vitess.io/vitess/go/vt/vtgate/vtgateservice"
	"vitess.io/vitess/go/vt/vttls"
)

var (
	mysqlServerPort                   = -1
	mysqlServerBindAddress            string
	mysqlServerSocketPath             string
	mysqlTCPVersion                   = "tcp"
	mysqlAuthServerImpl               = "static"
	mysqlAllowClearTextWithoutTLS     bool
	mysqlProxyProtocol                bool
	mysqlServerRequireSecureTransport bool
	mysqlSslCert                      string
	mysqlSslKey                       string
	mysqlSslCa                        string
	mysqlSslCrl                       string
	mysqlSslServerCA                  string
	mysqlTLSMinVersion                string

	mysqlKeepAlivePeriod          time.Duration
	mysqlConnReadTimeout          time.Duration
	mysqlConnWriteTimeout         time.Duration
	mysqlQueryTimeout             time.Duration
	mysqlSlowConnectWarnThreshold time.Duration
	mysqlConnBufferPooling        bool

	mysqlDefaultWorkloadName = "OLTP"
	mysqlDefaultWorkload     int32
	mysqlDrainOnTerm         bool

	mysqlServerFlushDelay = 100 * time.Millisecond
	mysqlServerMultiQuery = false
)

func registerPluginFlags(fs *pflag.FlagSet) {
	utils.SetFlagIntVar(fs, &mysqlServerPort, "mysql-server-port", mysqlServerPort, "If set, also listen for MySQL binary protocol connections on this port.")
	utils.SetFlagStringVar(fs, &mysqlServerBindAddress, "mysql-server-bind-address", mysqlServerBindAddress, "Binds on this address when listening to MySQL binary protocol. Useful to restrict listening to 'localhost' only for instance.")
	utils.SetFlagStringVar(fs, &mysqlServerSocketPath, "mysql-server-socket-path", mysqlServerSocketPath, "This option specifies the Unix socket file to use when listening for local connections. By default it will be empty and it won't listen to a unix socket")
	utils.SetFlagStringVar(fs, &mysqlTCPVersion, "mysql-tcp-version", mysqlTCPVersion, "Select tcp, tcp4, or tcp6 to control the socket type.")
	utils.SetFlagStringVar(fs, &mysqlAuthServerImpl, "mysql-auth-server-impl", mysqlAuthServerImpl, "Which auth server implementation to use. Options: none, ldap, clientcert, static, vault.")
	utils.SetFlagBoolVar(fs, &mysqlAllowClearTextWithoutTLS, "mysql-allow-clear-text-without-tls", mysqlAllowClearTextWithoutTLS, "If set, the server will allow the use of a clear text password over non-SSL connections.")
	utils.SetFlagBoolVar(fs, &mysqlProxyProtocol, "proxy-protocol", mysqlProxyProtocol, "Enable HAProxy PROXY protocol on MySQL listener socket")
	utils.SetFlagBoolVar(fs, &mysqlServerRequireSecureTransport, "mysql-server-require-secure-transport", mysqlServerRequireSecureTransport, "Reject insecure connections but only if mysql-server-ssl-cert and mysql-server-ssl-key are provided")
	utils.SetFlagStringVar(fs, &mysqlSslCert, "mysql-server-ssl-cert", mysqlSslCert, "Path to the ssl cert for mysql server plugin SSL")
	utils.SetFlagStringVar(fs, &mysqlSslKey, "mysql-server-ssl-key", mysqlSslKey, "Path to ssl key for mysql server plugin SSL")
	utils.SetFlagStringVar(fs, &mysqlSslCa, "mysql-server-ssl-ca", mysqlSslCa, "Path to ssl CA for mysql server plugin SSL. If specified, server will require and validate client certs.")
	utils.SetFlagStringVar(fs, &mysqlSslCrl, "mysql-server-ssl-crl", mysqlSslCrl, "Path to ssl CRL for mysql server plugin SSL")
	utils.SetFlagStringVar(fs, &mysqlTLSMinVersion, "mysql-server-tls-min-version", mysqlTLSMinVersion, "Configures the minimal TLS version negotiated when SSL is enabled. Defaults to TLSv1.2. Options: TLSv1.0, TLSv1.1, TLSv1.2, TLSv1.3.")
	utils.SetFlagStringVar(fs, &mysqlSslServerCA, "mysql-server-ssl-server-ca", mysqlSslServerCA, "path to server CA in PEM format, which will be combine with server cert, return full certificate chain to clients")
	utils.SetFlagDurationVar(fs, &mysqlSlowConnectWarnThreshold, "mysql-slow-connect-warn-threshold", mysqlSlowConnectWarnThreshold, "Warn if it takes more than the given threshold for a mysql connection to establish")
	utils.SetFlagDurationVar(fs, &mysqlConnReadTimeout, "mysql-server-read-timeout", mysqlConnReadTimeout, "connection read timeout")
	utils.SetFlagDurationVar(fs, &mysqlConnWriteTimeout, "mysql-server-write-timeout", mysqlConnWriteTimeout, "connection write timeout")
	utils.SetFlagDurationVar(fs, &mysqlQueryTimeout, "mysql-server-query-timeout", mysqlQueryTimeout, "mysql query timeout")
	fs.BoolVar(&mysqlConnBufferPooling, "mysql-server-pool-conn-read-buffers", mysqlConnBufferPooling, "If set, the server will pool incoming connection read buffers")
	fs.DurationVar(&mysqlKeepAlivePeriod, "mysql-server-keepalive-period", mysqlKeepAlivePeriod, "TCP period between keep-alives")
	utils.SetFlagDurationVar(fs, &mysqlServerFlushDelay, "mysql-server-flush-delay", mysqlServerFlushDelay, "Delay after which buffered response will be flushed to the client.")
	utils.SetFlagStringVar(fs, &mysqlDefaultWorkloadName, "mysql-default-workload", mysqlDefaultWorkloadName, "Default session workload (OLTP, OLAP, DBA)")
	fs.BoolVar(&mysqlDrainOnTerm, "mysql-server-drain-onterm", mysqlDrainOnTerm, "If set, the server waits for --onterm-timeout for already connected clients to complete their in flight work")
	utils.SetFlagBoolVar(fs, &mysqlServerMultiQuery, "mysql-server-multi-query-protocol", mysqlServerMultiQuery, "If set, the server will use the new implementation of handling queries where-in multiple queries are sent together.")
}

// tempTableHeartbeatTarget identifies one reserved connection that must be
// kept alive because its session holds temporary tables. Target slices are
// replaced wholesale under tempTableConn.mu, never mutated in place.
type tempTableHeartbeatTarget struct {
	target     *querypb.Target
	alias      *topodatapb.TabletAlias
	reservedID int64
	// failures counts consecutive keepalive failures. It only gates the
	// transition logging (warn once when a target starts failing, note the
	// recovery once when it stops): failures never evict a target — only a
	// confirmed connection-closed error does — or a few seconds of network
	// trouble would silently disable the keepalives of a live connection.
	// It resets on success (and whenever a command's end hook republishes
	// the targets).
	failures int
}

// tempTableConn coordinates a client connection's command goroutine with the
// background heartbeat sweeper. The command goroutine holds mu for the
// duration of every command, so a keepalive can never race a command on the
// same reserved connection at the tablet; the sweeper only beats a connection
// whose mu it can TryLock, skipping those with a command in flight (whose own
// execution keeps the reserved connections alive).
type tempTableConn struct {
	// mu guards gen, targets, and closed. It is held only for the
	// microseconds it takes to snapshot or replace the targets — never
	// across a beat RPC — so a slow keepalive cannot delay foreground
	// commands here, and keepalives are never starved by a long command.
	// (On the tablet, a command colliding with a keepalive's own
	// microseconds-long hold of the reserved connection waits it out; see
	// TxPool.GetAndLock.)
	mu sync.Mutex
	// gen increments whenever the connection's own goroutine replaces the
	// targets (command end, reset, close). A sweep snapshots targets and
	// gen, sends its beats without holding mu, and applies the results only
	// if gen is unchanged — results for a superseded snapshot are discarded.
	gen     uint64
	targets []tempTableHeartbeatTarget
	// closed marks the connection as shutting down so no further beats
	// start; beats tracks the in-flight ones so close can wait them out
	// before the session's reserved connections are released. Add only
	// happens under mu while !closed, so it cannot race Wait.
	closed bool
	beats  sync.WaitGroup
}

// vtgateHandler implements the Listener interface.
// It stores the Session in the ClientData of a Connection.
type vtgateHandler struct {
	mysql.UnimplementedHandler
	mu sync.Mutex

	vtg         *VTGate
	connections map[uint32]*mysql.Conn

	// tempTableConns maps *mysql.Conn to its *tempTableConn. An entry exists
	// only while the connection's session holds temporary tables, so the
	// background sweeper can keep its reserved connections (and their mysqld
	// connections) alive. Entries are added and removed by the connection's
	// own goroutine; the sweeper also evicts targets that no longer exist.
	tempTableConns sync.Map

	busyConnections atomic.Int32
}

type vtgateMySQLConnection struct {
	handler         *vtgateHandler
	conn            *mysql.Conn
	slowQueryStates []bool
}

func (vmc *vtgateMySQLConnection) KillQuery(connectionID uint32) error {
	return vmc.handler.KillQuery(connectionID)
}

func (vmc *vtgateMySQLConnection) KillConnection(ctx context.Context, connectionID uint32) error {
	return vmc.handler.KillConnection(ctx, connectionID)
}

func (vmc *vtgateMySQLConnection) SetQueryWasSlow(slow bool) {
	setSlowQueryStatus(vmc.conn, slow)
	vmc.slowQueryStates = append(vmc.slowQueryStates, slow)
}

func (vmc *vtgateMySQLConnection) IngressBytes() uint64 {
	return vmc.conn.IngressBytes()
}

func newVtgateHandler(vtg *VTGate) *vtgateHandler {
	return &vtgateHandler{
		vtg:         vtg,
		connections: make(map[uint32]*mysql.Conn),
	}
}

// tempTableHeartbeatTargets extracts the reserved connections that need
// keepalives from a session that holds temporary tables.
func tempTableHeartbeatTargets(session *vtgatepb.Session) []tempTableHeartbeatTarget {
	if !session.GetOptions().GetHasCreatedTempTables() {
		return nil
	}
	shardSessions := session.GetShardSessions()
	targets := make([]tempTableHeartbeatTarget, 0, len(shardSessions))
	for _, ss := range shardSessions {
		// Shard sessions with an open transaction are deliberately excluded:
		// the tablet does not reset its transaction timer for activity on an
		// in-transaction connection (idle transactions are supposed to be
		// killed at the transaction timeout, temp tables or not), so a beat
		// would not keep the connection alive — it would only inject a query
		// into the user's open transaction. When the transaction commits, the
		// command's end hook republishes the target and keepalives resume.
		if ss.GetReservedId() != 0 && ss.GetTransactionId() == 0 {
			targets = append(targets, tempTableHeartbeatTarget{
				target:     ss.GetTarget(),
				alias:      ss.GetTabletAlias(),
				reservedID: ss.GetReservedId(),
			})
		}
	}
	return targets
}

// tempTableCommandEnd must be called once a client command settles: it
// republishes the session's keepalive targets (registering or deregistering
// the connection as needed) and bumps the generation so any sweep started
// against the previous targets discards its results.
func (vh *vtgateHandler) tempTableCommandEnd(c *mysql.Conn) {
	targets := tempTableHeartbeatTargets(vh.session(c))
	v, ok := vh.tempTableConns.Load(c)
	if !ok {
		if len(targets) == 0 {
			return
		}
		// A session-level transaction timeout at or below the heartbeat
		// interval reclaims the reserved connection between beats: the
		// keepalive cannot protect this session's temp tables. This is
		// intentional — an explicit session-level timeout is a per-session
		// choice that wins over the keepalive, unlike the global default,
		// which the feature deliberately outlives. Surface it once, at
		// registration.
		session := vh.session(c)
		if to := time.Duration(session.GetOptions().GetTransactionTimeout()) * time.Millisecond; to > 0 && to <= tempTableHeartbeatTime {
			log.Warn("session transaction timeout is not above the temp-table heartbeat interval; its temporary tables may still be reclaimed between heartbeats",
				slog.Duration("session_transaction_timeout", to),
				slog.Duration("temp_table_heartbeat_time", tempTableHeartbeatTime))
		}
		// The session acquired temporary tables during this command: register
		// the connection so the background sweeper keeps it alive.
		vh.tempTableConns.Store(c, &tempTableConn{targets: targets})
		return
	}
	ttc := v.(*tempTableConn)
	ttc.mu.Lock()
	defer ttc.mu.Unlock()
	ttc.gen++
	if len(targets) == 0 {
		// The temporary tables are gone (session reset or reserved
		// connections released): deregister.
		ttc.targets = nil
		vh.tempTableConns.Delete(c)
		return
	}
	ttc.targets = targets
	// Re-register in case the sweeper deregistered the entry after evicting
	// its last target during the command.
	vh.tempTableConns.Store(c, ttc)
}

// stopTempTableHeartbeats deregisters the connection and waits out any beat
// in flight, so the caller can safely release the session's reserved
// connections afterwards without a beat racing the release at the tablet.
func (vh *vtgateHandler) stopTempTableHeartbeats(c *mysql.Conn) {
	v, ok := vh.tempTableConns.Load(c)
	if !ok {
		return
	}
	ttc := v.(*tempTableConn)
	ttc.mu.Lock()
	ttc.closed = true
	ttc.gen++
	ttc.targets = nil
	vh.tempTableConns.Delete(c)
	ttc.mu.Unlock()
	ttc.beats.Wait()
}

// startTempTableHeartbeat launches the background sweeper that keeps reserved
// connections holding temporary tables alive. It stops when ctx is cancelled.
func (vh *vtgateHandler) startTempTableHeartbeat(ctx context.Context) {
	if tempTableHeartbeatTime <= 0 {
		log.Info("temp-table connection heartbeats are disabled",
			slog.Duration("temp_table_heartbeat_time", tempTableHeartbeatTime))
		return
	}
	// Each connection is beaten once per heartbeat interval, but the work is
	// spread across buckets (by connection ID) so a large number of
	// registered connections produces a steady trickle of touch RPCs rather
	// than a spike on every interval. Each bucket runs on its own goroutine,
	// staggered by an offset, so a bucket full of slow or unreachable
	// tablets delays only its own next round — never another bucket's
	// keepalives or the overall schedule.
	interval := tempTableHeartbeatTime / tempTableBeatBuckets
	for bucket := range uint32(tempTableBeatBuckets) {
		go func() {
			// Stagger the buckets across the interval.
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(bucket) * interval):
			}
			ticker := time.NewTicker(tempTableHeartbeatTime)
			defer ticker.Stop()
			for {
				vh.sweepTempTableBucket(ctx, bucket)
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}
}

// sendTempTableHeartbeats pings the reserved connections of every registered
// client connection, fanning out per connection so one slow tablet cannot
// delay the keepalives of the others. It returns once all beats settle.
// tempTableBeatBuckets is how many slices the heartbeat interval is divided
// into: each registered connection belongs to one bucket (by connection ID)
// and is beaten once per interval, on its bucket's tick. Staggering bounds
// the instantaneous goroutine and RPC load without queueing one session's
// keepalives behind another's slow tablet.
const tempTableBeatBuckets = 16

// sendTempTableHeartbeats beats every registered connection (all buckets).
// The background sweeper beats one bucket at a time instead; this form is
// for tests and returns once all beats settle.
func (vh *vtgateHandler) sendTempTableHeartbeats(ctx context.Context) {
	for bucket := range uint32(tempTableBeatBuckets) {
		vh.sweepTempTableBucket(ctx, bucket)
	}
}

// tempTableBeatPerTabletConcurrency bounds how many of a single tablet's
// connections a bucket sweep beats at once. Concurrency across the whole
// sweep is therefore bounded by (distinct tablets in the bucket) *
// tempTableBeatPerTabletConcurrency — a deployment has a bounded number of
// tablets, so this caps goroutines and in-flight touch RPCs regardless of
// how many client connections are registered.
const tempTableBeatPerTabletConcurrency = 8

// sweepTempTableBucket beats the reserved connections of the registered
// client connections in one bucket, grouped by tablet. Each tablet is beaten
// on its own goroutine (with a bounded pool per tablet), so a slow or
// unreachable tablet delays only its own connections' keepalives — never a
// healthy tablet's. Otherwise a wave of connections pointing at a stalled
// tablet could occupy every worker and let a healthy session's temporary
// tables expire behind them. Each bucket also sweeps on its own goroutine,
// so a slow bucket does not delay another bucket's keepalives.
func (vh *vtgateHandler) sweepTempTableBucket(ctx context.Context, bucket uint32) {
	byTablet := make(map[string][]*mysql.Conn)
	vh.tempTableConns.Range(func(key, value any) bool {
		c := key.(*mysql.Conn)
		if c.ConnectionID%tempTableBeatBuckets != bucket {
			return true
		}
		ttc := value.(*tempTableConn)
		ttc.mu.Lock()
		if !ttc.closed && len(ttc.targets) > 0 {
			// Group by the connection's first reserved target's tablet. A
			// session almost always holds a single reserved connection, so
			// this is its tablet; a session reserved on several shards is
			// grouped by the first and still beaten as a unit.
			alias := topoproto.TabletAliasString(ttc.targets[0].alias)
			byTablet[alias] = append(byTablet[alias], c)
		}
		ttc.mu.Unlock()
		return true
	})
	if len(byTablet) == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, conns := range byTablet {
		wg.Go(func() {
			work := make(chan *mysql.Conn)
			var inner sync.WaitGroup
			for range min(tempTableBeatPerTabletConcurrency, len(conns)) {
				inner.Go(func() {
					for c := range work {
						vh.beatOneTempTableConn(ctx, c)
					}
				})
			}
			for _, c := range conns {
				work <- c
			}
			close(work)
			inner.Wait()
		})
	}
	wg.Wait()
}

// beatOneTempTableConn beats a single registered connection. The in-flight
// beat is counted (via ttc.beats) only once a worker actually picks the
// connection up, not while it waits in the queue, so closing a connection
// waits out only its running beat — never work still queued behind the pool.
func (vh *vtgateHandler) beatOneTempTableConn(ctx context.Context, c *mysql.Conn) {
	v, ok := vh.tempTableConns.Load(c)
	if !ok {
		return
	}
	ttc := v.(*tempTableConn)
	ttc.mu.Lock()
	if ttc.closed || len(ttc.targets) == 0 {
		ttc.mu.Unlock()
		return
	}
	gen := ttc.gen
	targets := slices.Clone(ttc.targets)
	ttc.beats.Add(1)
	ttc.mu.Unlock()
	defer ttc.beats.Done()
	vh.beatTempTableConn(ctx, c, ttc, gen, targets)
}

// tempTableBeatContext returns ctx carrying the client's caller identity,
// built exactly as the command path builds it: with
// --queryserver-config-strict-table-acl the tablet rejects any query without
// an immediate caller id ("missing caller id"), which would make every beat
// fail (and not as a connection-closed error), so the reserved connection
// would still be reclaimed. It also attributes the beats to the owning user
// in the tablet's query logs. The identity fields on the connection are set
// at authentication time and immutable after, so reading them from the
// sweeper is safe.
func tempTableBeatContext(ctx context.Context, c *mysql.Conn) context.Context {
	if c.UserData == nil {
		return ctx
	}
	return callerid.NewContext(ctx,
		callerid.NewEffectiveCallerID(
			c.User,                  /* principal: who */
			c.RemoteAddr().String(), /* component: running client process */
			"VTGate MySQL Connector" /* subcomponent: part of the client */),
		c.UserData.Get())
}

// beatTempTableConn sends one keepalive per reserved connection registered
// for a client connection. Reserved connections that no longer exist are
// evicted so they are not beaten (and warned about) on every sweep; the
// session re-registers live ones at the end of its next command.
// beatTempTableConn beats the given snapshot of a connection's keepalive
// targets without holding the connection's mutex across the RPCs: commands
// run concurrently with beats (on the tablet, a keepalive that finds the
// reserved connection busy counts it as alive, and a command that collides
// with a keepalive's microseconds-long hold waits it out).
// The results are applied only if gen still matches — a command end, reset,
// or close that replaced the targets in the meantime supersedes them.
func (vh *vtgateHandler) beatTempTableConn(ctx context.Context, c *mysql.Conn, ttc *tempTableConn, gen uint64, targets []tempTableHeartbeatTarget) {
	ctx = tempTableBeatContext(ctx, c)
	// Each target gets its own budget of half the heartbeat interval, and
	// the targets are beaten concurrently: one hanging tablet must not eat
	// a shared budget and starve the keepalives of the session's other
	// reserved connections. At short heartbeat intervals half the interval
	// would leave a sub-second budget that a momentarily slow (but healthy)
	// tablet can miss, so the budget never drops below a floor; missing a
	// tick because a round ran long is harmless (the ticker just skips).
	// On tablets that predate the keepalive option, the beat executes as a
	// real query, where a deadline expiring mid-execution kills only the
	// query (KILL QUERY), so the reserved connection and its temp tables
	// survive.
	budget := max(tempTableHeartbeatTime/2, 2*time.Second)
	errs := make([]error, len(targets))
	var wg sync.WaitGroup
	for i, t := range targets {
		wg.Go(func() {
			bctx, cancel := context.WithTimeout(ctx, budget)
			defer cancel()
			errs[i] = vh.sendTempTableBeat(bctx, t)
		})
	}
	wg.Wait()
	kept := make([]tempTableHeartbeatTarget, 0, len(targets))
	for i, t := range targets {
		err := errs[i]
		if err == nil {
			if t.failures > 0 {
				log.Info("temp-table connection heartbeat recovered",
					slog.Int64("reserved_id", t.reservedID),
					slog.String("tablet", topoproto.TabletAliasString(t.alias)),
					slog.Int("failed_beats", t.failures))
			}
			t.failures = 0
			kept = append(kept, t)
			continue
		}
		if wasConnectionClosed(err) {
			// The reserved connection is gone (e.g. already reclaimed by the
			// tablet): stop beating it. The session self-heals by
			// re-reserving on its next query.
			log.Warn("temp-table connection is gone, stopping its keepalives",
				slog.Int64("reserved_id", t.reservedID),
				slog.String("tablet", topoproto.TabletAliasString(t.alias)),
				slog.Any("error", err))
			continue
		}
		// Transient failure (e.g. slow or unreachable tablet): keep the
		// target and retry on the next sweep — for as long as it takes.
		// Only a confirmed connection-closed error may evict; a failure
		// count must not, or a few seconds of network trouble would
		// silently disable the keepalives of a live connection whose temp
		// tables the tablet would then reclaim. To keep an unreachable
		// tablet from producing a warning every sweep indefinitely, log on
		// state transitions only: once when a target starts failing (and
		// once above when it recovers).
		t.failures++
		if t.failures == 1 {
			log.Warn("temp-table connection heartbeat failed, will keep retrying",
				slog.Int64("reserved_id", t.reservedID),
				slog.String("tablet", topoproto.TabletAliasString(t.alias)),
				slog.Any("error", err))
		}
		kept = append(kept, t)
	}
	ttc.mu.Lock()
	defer ttc.mu.Unlock()
	if ttc.gen != gen {
		// The targets were replaced while this round was in flight (command
		// end, reset, or close): its results describe a superseded snapshot.
		return
	}
	ttc.targets = kept
	if len(kept) == 0 {
		vh.tempTableConns.Delete(c)
	}
}

// sendTempTableBeat sends a keepalive touch for a single reserved
// connection: the tablet refreshes the connection's idle timers without
// executing anything on the underlying MySQL connection, so mysqld's
// wait_timeout keeps counting only real user traffic and reclaims idle
// sessions exactly as MySQL would. The "select 1" only executes on tablets
// that predate the keepalive option (they ignore it), where the beat then
// also resets mysqld's idle timer until the tablet is upgraded.
func (vh *vtgateHandler) sendTempTableBeat(ctx context.Context, t tempTableHeartbeatTarget) error {
	qs, err := vh.vtg.gw.QueryServiceByAlias(ctx, t.alias, t.target)
	if err != nil {
		return err
	}
	options := &querypb.ExecuteOptions{ReservedConnKeepAlive: true}
	_, err = qs.Execute(ctx, nil, t.target, "/* temp-table keepalive */ select 1", nil, 0 /* transactionID */, t.reservedID, options)
	return err
}

func (vh *vtgateHandler) NewConnection(c *mysql.Conn) {
	// Match VTGate's default session state (Autocommit: true) so the
	// handshake packet reports correct status flags to the client.
	c.StatusFlags |= mysql.ServerStatusAutocommit

	vh.mu.Lock()
	defer vh.mu.Unlock()
	vh.connections[c.ConnectionID] = c
}

func (vh *vtgateHandler) numConnections() int {
	vh.mu.Lock()
	defer vh.mu.Unlock()
	return len(vh.connections)
}

func (vh *vtgateHandler) ComResetConnection(c *mysql.Conn) {
	// The command-end hook republishes the temp-table keepalive targets once
	// the session is released: it deregisters the connection and supersedes
	// any beat in flight.
	defer vh.tempTableCommandEnd(c)

	ctx := context.Background()
	session := vh.session(c)
	if session.InTransaction {
		defer vh.busyConnections.Add(-1)
	}
	err := vh.vtg.CloseSession(ctx, session)
	if err != nil {
		log.Error(fmt.Sprintf("Error happened in transaction rollback: %v", err))
	}
	// The reset released the reserved connections, and the temporary tables
	// and applied session settings died with them — but the session object
	// survives on the connection, so clear vtgate's record of that state.
	// Otherwise the session would still look like a temp-table holder (the
	// next reserved shard session would re-register it for heartbeats, and
	// query-plan caching would stay disabled for the rest of the
	// connection's life), and every subsequent query would be forced onto a
	// pointless fresh reserved connection. InReservedConn and
	// SystemVariables must be cleared together: the flag plus the recorded
	// variables are what re-establish settings on newly reserved
	// connections, so clearing one without the other would desynchronize
	// what the session reports (@@var reads) from what its connections
	// actually have applied.
	if opts := session.GetOptions(); opts != nil {
		opts.HasCreatedTempTables = false
	}
	session.InReservedConn = false
	session.SystemVariables = nil
}

func (vh *vtgateHandler) ConnectionClosed(c *mysql.Conn) {
	// Stop temp-table keepalives first, waiting out any beat in flight, so a
	// beat cannot race the release of the reserved connections below.
	vh.stopTempTableHeartbeats(c)

	// Rollback if there is an ongoing transaction. Ignore error.
	defer func() {
		vh.mu.Lock()
		delete(vh.connections, c.ConnectionID)
		vh.mu.Unlock()
	}()

	var ctx context.Context
	var cancel context.CancelFunc
	if mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(context.Background(), mysqlQueryTimeout)
		defer cancel()
	} else {
		ctx = context.Background()
	}
	session := vh.session(c)
	if session.InTransaction {
		defer vh.busyConnections.Add(-1)
	}
	_ = vh.vtg.CloseSession(ctx, session)
}

// Regexp to extract parent span id over the sql query
var r = regexp.MustCompile(`/\*VT_SPAN_CONTEXT=(.*?)\*/`)

// this function is here to make this logic easy to test by decoupling the logic from the `trace.NewSpan` and `trace.NewFromString` functions
func startSpanTestable(ctx context.Context, query, label string,
	newSpan func(context.Context, string) (trace.Span, context.Context),
	newSpanFromString func(context.Context, string, string) (trace.Span, context.Context, error),
) (trace.Span, context.Context, error) {
	_, comments := sqlparser.SplitMarginComments(query)
	match := r.FindStringSubmatch(comments.Leading)
	span, ctx := getSpan(ctx, match, newSpan, label, newSpanFromString)

	trace.AnnotateSQL(span, sqlparser.Preview(query))

	return span, ctx, nil
}

func getSpan(ctx context.Context, match []string, newSpan func(context.Context, string) (trace.Span, context.Context), label string, newSpanFromString func(context.Context, string, string) (trace.Span, context.Context, error)) (trace.Span, context.Context) {
	var span trace.Span
	if len(match) != 0 {
		var err error
		span, ctx, err = newSpanFromString(ctx, match[1], label)
		if err == nil {
			return span, ctx
		}
		log.Warn("Unable to parse VT_SPAN_CONTEXT: " + err.Error())
	}
	span, ctx = newSpan(ctx, label)
	return span, ctx
}

func startSpan(ctx context.Context, query, label string) (trace.Span, context.Context, error) {
	return startSpanTestable(ctx, query, label, trace.NewSpan, trace.NewFromString)
}

// extractSpanContext extracts the VT_SPAN_CONTEXT value from a query's leading comments.
// Returns empty string if no span context is found.
func extractSpanContext(query string) string {
	_, comments := sqlparser.SplitMarginComments(query)
	match := r.FindStringSubmatch(comments.Leading)
	if len(match) != 0 {
		return match[1]
	}
	return ""
}

// startSpanFromPrepareTestable creates a span for a prepared statement execution,
// caching the extracted VT_SPAN_CONTEXT on the PrepareData to avoid re-parsing
// the SQL comments on every execution.
func startSpanFromPrepareTestable(ctx context.Context, prepare *mysql.PrepareData, label string,
	newSpan func(context.Context, string) (trace.Span, context.Context),
	newSpanFromString func(context.Context, string, string) (trace.Span, context.Context, error),
) (trace.Span, context.Context, error) {
	if prepare.SpanContext == nil {
		sc := extractSpanContext(prepare.PrepareStmt)
		prepare.SpanContext = &sc
	}

	var span trace.Span
	if *prepare.SpanContext != "" {
		var err error
		span, ctx, err = newSpanFromString(ctx, *prepare.SpanContext, label)
		if err == nil {
			trace.AnnotateSQL(span, sqlparser.Preview(prepare.PrepareStmt))
			return span, ctx, nil
		}
		log.Warn("Unable to parse VT_SPAN_CONTEXT", slog.Any("error", err))
		// Clear the cached value so subsequent executions skip the parse attempt.
		*prepare.SpanContext = ""
	}
	span, ctx = newSpan(ctx, label)
	trace.AnnotateSQL(span, sqlparser.Preview(prepare.PrepareStmt))
	return span, ctx, nil
}

func startSpanFromPrepare(ctx context.Context, prepare *mysql.PrepareData, label string) (trace.Span, context.Context, error) {
	return startSpanFromPrepareTestable(ctx, prepare, label, trace.NewSpan, trace.NewFromString)
}

func (vh *vtgateHandler) ComQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	session := vh.session(c)
	if c.IsShuttingDown() && !session.InTransaction {
		c.MarkForClose()
		return sqlerror.NewSQLError(sqlerror.ERServerShutdown, sqlerror.SSNetError, "Server shutdown in progress")
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.UpdateCancelCtx(cancel)

	if mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, mysqlQueryTimeout)
		defer cancel()
	}

	span, ctx, err := startSpan(ctx, query, "vtgateHandler.ComQuery")
	if err != nil {
		return vterrors.Wrap(err, "failed to extract span")
	}
	defer span.Finish()

	// Lock out the temp-table heartbeat sweeper for the duration of the
	// command; once it settles, republish the session's heartbeat targets.
	defer vh.tempTableCommandEnd(c)

	ctx = callinfo.MysqlCallInfo(ctx, c)

	// Fill in the ImmediateCallerID with the UserData returned by
	// the AuthServer plugin for that user. If nothing was
	// returned, use the User. This lets the plugin map a MySQL
	// user used for authentication to a Vitess User used for
	// Table ACLs and Vitess authentication in general.
	im := c.UserData.Get()
	ef := callerid.NewEffectiveCallerID(
		c.User,                  /* principal: who */
		c.RemoteAddr().String(), /* component: running client process */
		"VTGate MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)
	mysqlCtx := &vtgateMySQLConnection{handler: vh, conn: c}

	if !session.InTransaction {
		vh.busyConnections.Add(1)
	}
	defer func() {
		if !session.InTransaction {
			vh.busyConnections.Add(-1)
		}
	}()

	if session.Options.Workload == querypb.ExecuteOptions_OLAP {
		streamCallback, deferredResult := deferFirstOKOnlyResult(callback)
		session, err := vh.vtg.StreamExecute(ctx, mysqlCtx, session, query, make(map[string]*querypb.BindVariable), false, streamCallback)
		if err != nil {
			return sqlerror.NewSQLErrorFromError(err)
		}
		fillInTxStatusFlags(c, session)
		if result := deferredResult(); result != nil {
			return callback(result)
		}
		return nil
	}
	session, result, err := vh.vtg.Execute(ctx, mysqlCtx, session, query, make(map[string]*querypb.BindVariable), false)

	if err := sqlerror.NewSQLErrorFromError(err); err != nil {
		return err
	}
	fillInTxStatusFlags(c, session)
	return callback(result)
}

// ComQueryMulti is a newer version of ComQuery that supports running multiple queries in a single call.
func (vh *vtgateHandler) ComQueryMulti(c *mysql.Conn, sql string, callback func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error) error {
	session := vh.session(c)
	if c.IsShuttingDown() && !session.InTransaction {
		c.MarkForClose()
		return sqlerror.NewSQLError(sqlerror.ERServerShutdown, sqlerror.SSNetError, "Server shutdown in progress")
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.UpdateCancelCtx(cancel)

	span, ctx, err := startSpan(ctx, sql, "vtgateHandler.ComQueryMulti")
	if err != nil {
		return vterrors.Wrap(err, "failed to extract span")
	}
	defer span.Finish()

	// Lock out the temp-table heartbeat sweeper for the duration of the
	// command; once it settles, republish the session's heartbeat targets.
	defer vh.tempTableCommandEnd(c)

	ctx = callinfo.MysqlCallInfo(ctx, c)

	// Fill in the ImmediateCallerID with the UserData returned by
	// the AuthServer plugin for that user. If nothing was
	// returned, use the User. This lets the plugin map a MySQL
	// user used for authentication to a Vitess User used for
	// Table ACLs and Vitess authentication in general.
	im := c.UserData.Get()
	ef := callerid.NewEffectiveCallerID(
		c.User,                  /* principal: who */
		c.RemoteAddr().String(), /* component: running client process */
		"VTGate MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)
	mysqlCtx := &vtgateMySQLConnection{handler: vh, conn: c}

	if !session.InTransaction {
		vh.busyConnections.Add(1)
	}
	defer func() {
		if !session.InTransaction {
			vh.busyConnections.Add(-1)
		}
	}()

	if session.Options.Workload == querypb.ExecuteOptions_OLAP {
		if c.Capabilities&mysql.CapabilityClientMultiStatements != 0 {
			session, err = vh.streamExecuteMultiQuery(ctx, c, mysqlCtx, session, sql, callback)
		} else {
			firstPacket := true
			var deferredResult *sqltypes.Result
			session, err = vh.vtg.StreamExecute(ctx, mysqlCtx, session, sql, make(map[string]*querypb.BindVariable), false, func(result *sqltypes.Result) error {
				if firstPacket && len(result.Fields) == 0 {
					deferredResult = result
					firstPacket = false
					return nil
				}
				defer func() {
					firstPacket = false
				}()
				return callback(sqltypes.QueryResponse{QueryResult: result}, false, firstPacket)
			})
			if err == nil && deferredResult != nil {
				fillInTxStatusFlags(c, session)
				return callback(sqltypes.QueryResponse{QueryResult: deferredResult}, false, true)
			}
		}
		if err != nil {
			return sqlerror.NewSQLErrorFromError(err)
		}
		fillInTxStatusFlags(c, session)
		return nil
	}
	var results []*sqltypes.Result
	var result *sqltypes.Result
	var queryResults []sqltypes.QueryResponse
	if c.Capabilities&mysql.CapabilityClientMultiStatements != 0 {
		session, results, err = vh.vtg.ExecuteMulti(ctx, mysqlCtx, session, sql)
		for _, res := range results {
			queryResults = append(queryResults, sqltypes.QueryResponse{QueryResult: res})
		}
		if err != nil {
			queryResults = append(queryResults, sqltypes.QueryResponse{QueryError: sqlerror.NewSQLErrorFromError(err)})
		}
	} else {
		session, result, err = vh.vtg.Execute(ctx, mysqlCtx, session, sql, make(map[string]*querypb.BindVariable), false)
		queryResults = append(queryResults, sqltypes.QueryResponse{QueryResult: result, QueryError: sqlerror.NewSQLErrorFromError(err)})
	}

	fillInTxStatusFlags(c, session)
	for idx, res := range queryResults {
		applyMultiQueryStatusFlags(c, mysqlCtx.slowQueryStates, idx)
		if callbackErr := callback(res, idx < len(queryResults)-1, true); callbackErr != nil {
			return callbackErr
		}
	}
	return nil
}

func (vh *vtgateHandler) streamExecuteMultiQuery(ctx context.Context, c *mysql.Conn, mysqlCtx *vtgateMySQLConnection, session *vtgatepb.Session, sql string, callback func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error) (*vtgatepb.Session, error) {
	queries, err := vh.vtg.executor.Environment().Parser().SplitStatementToPieces(sql)
	if err != nil {
		return session, err
	}
	if len(queries) == 0 {
		return session, sqlparser.ErrEmpty
	}
	queryIngressBytes := queryIngressBytesForStatements(ctx, mysqlCtx, queries)
	for idx, query := range queries {
		firstPacket := true
		more := idx < len(queries)-1
		var deferredResult *sqltypes.Result
		func() {
			queryCtx := ctx
			if queryIngressBytes != nil {
				queryCtx = vtgateservice.ContextWithIngressBytes(queryCtx, queryIngressBytes[idx])
			}
			var cancel context.CancelFunc
			if mysqlQueryTimeout != 0 {
				queryCtx, cancel = context.WithTimeout(queryCtx, mysqlQueryTimeout)
				defer cancel()
			}
			session, err = vh.vtg.StreamExecute(queryCtx, mysqlCtx, session, query, make(map[string]*querypb.BindVariable), false, func(result *sqltypes.Result) error {
				if firstPacket && len(result.Fields) == 0 {
					deferredResult = result
					firstPacket = false
					return nil
				}
				if firstPacket {
					applyMultiQueryStatusFlags(c, mysqlCtx.slowQueryStates, idx)
				}
				defer func() {
					firstPacket = false
				}()
				return callback(sqltypes.QueryResponse{QueryResult: result}, more, firstPacket)
			})
		}()
		if err != nil {
			if firstPacket {
				applyMultiQueryStatusFlags(c, mysqlCtx.slowQueryStates, idx)
				return session, callback(sqltypes.QueryResponse{QueryError: sqlerror.NewSQLErrorFromError(err)}, false, true)
			}
			return session, err
		}
		if deferredResult != nil {
			previousStatusFlags := c.StatusFlags
			fillInTxStatusFlags(c, session)
			applyMultiQueryStatusFlagsWithPrevious(c, mysqlCtx.slowQueryStates, idx, previousStatusFlags)
			if err := callback(sqltypes.QueryResponse{QueryResult: deferredResult}, more, true); err != nil {
				return session, err
			}
		}
	}
	return session, nil
}

func fillInTxStatusFlags(c *mysql.Conn, session *vtgatepb.Session) {
	if session.InTransaction {
		c.StatusFlags |= mysql.ServerStatusInTrans
	} else {
		c.StatusFlags &= mysql.NoServerStatusInTrans
	}
	if session.Autocommit {
		c.StatusFlags |= mysql.ServerStatusAutocommit
	} else {
		c.StatusFlags &= mysql.NoServerStatusAutocommit
	}
}

func setSlowQueryStatus(c *mysql.Conn, slow bool) {
	c.StatusFlags = slowQueryStatusFlags(c.StatusFlags, slow)
}

func applyMultiQueryStatusFlags(c *mysql.Conn, slowQueryStates []bool, idx int) {
	applyMultiQueryStatusFlagsWithPrevious(c, slowQueryStates, idx, c.StatusFlags)
}

func applyMultiQueryStatusFlagsWithPrevious(c *mysql.Conn, slowQueryStates []bool, idx int, previousStatusFlags uint16) {
	if idx > 0 && idx-1 < len(slowQueryStates) {
		c.SetPendingMultiResultStatusFlags(slowQueryStatusFlags(previousStatusFlags, slowQueryStates[idx-1]))
	}
	if idx < len(slowQueryStates) {
		setSlowQueryStatus(c, slowQueryStates[idx])
	}
}

func slowQueryStatusFlags(statusFlags uint16, slow bool) uint16 {
	if slow {
		return statusFlags | mysql.ServerQueryWasSlow
	}
	return statusFlags &^ mysql.ServerQueryWasSlow
}

// ComPrepare is the handler for command prepare.
func (vh *vtgateHandler) ComPrepare(c *mysql.Conn, query string) ([]*querypb.Field, uint16, error) {
	var ctx context.Context
	var cancel context.CancelFunc
	if mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(context.Background(), mysqlQueryTimeout)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	// Lock out the temp-table heartbeat sweeper for the duration of the
	// command; once it settles, republish the session's heartbeat targets.
	defer vh.tempTableCommandEnd(c)

	ctx = callinfo.MysqlCallInfo(ctx, c)

	// Fill in the ImmediateCallerID with the UserData returned by
	// the AuthServer plugin for that user. If nothing was
	// returned, use the User. This lets the plugin map a MySQL
	// user used for authentication to a Vitess User used for
	// Table ACLs and Vitess authentication in general.
	im := c.UserData.Get()
	ef := callerid.NewEffectiveCallerID(
		c.User,                  /* principal: who */
		c.RemoteAddr().String(), /* component: running client process */
		"VTGate MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)
	ctx = vtgateservice.ContextWithIngressBytes(ctx, c.IngressBytes())

	session := vh.session(c)
	if !session.InTransaction {
		vh.busyConnections.Add(1)
	}
	defer func() {
		if !session.InTransaction {
			vh.busyConnections.Add(-1)
		}
	}()

	session, fld, paramsCount, err := vh.vtg.Prepare(ctx, session, query)
	err = sqlerror.NewSQLErrorFromError(err)
	if err != nil {
		return nil, 0, err
	}
	return fld, paramsCount, nil
}

func (vh *vtgateHandler) ComStmtExecute(c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	ctx, cancel := context.WithCancel(context.Background())
	c.UpdateCancelCtx(cancel)

	if mysqlQueryTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, mysqlQueryTimeout)
		defer cancel()
	}

	span, ctx, err := startSpanFromPrepare(ctx, prepare, "vtgateHandler.ComStmtExecute")
	if err != nil {
		return vterrors.Wrap(err, "failed to extract span")
	}
	defer span.Finish()

	// Lock out the temp-table heartbeat sweeper for the duration of the
	// command; once it settles, republish the session's heartbeat targets.
	defer vh.tempTableCommandEnd(c)

	ctx = callinfo.MysqlCallInfo(ctx, c)

	// Fill in the ImmediateCallerID with the UserData returned by
	// the AuthServer plugin for that user. If nothing was
	// returned, use the User. This lets the plugin map a MySQL
	// user used for authentication to a Vitess User used for
	// Table ACLs and Vitess authentication in general.
	im := c.UserData.Get()
	ef := callerid.NewEffectiveCallerID(
		c.User,                  /* principal: who */
		c.RemoteAddr().String(), /* component: running client process */
		"VTGate MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)
	mysqlCtx := &vtgateMySQLConnection{handler: vh, conn: c}

	session := vh.session(c)
	if !session.InTransaction {
		vh.busyConnections.Add(1)
	}
	defer func() {
		if !session.InTransaction {
			vh.busyConnections.Add(-1)
		}
	}()

	if session.Options.Workload == querypb.ExecuteOptions_OLAP {
		streamCallback, deferredResult := deferFirstOKOnlyResult(callback)
		_, err := vh.vtg.StreamExecute(ctx, mysqlCtx, session, prepare.PrepareStmt, prepare.BindVars, true, streamCallback)
		if err != nil {
			return sqlerror.NewSQLErrorFromError(err)
		}
		fillInTxStatusFlags(c, session)
		if result := deferredResult(); result != nil {
			return callback(result)
		}
		return nil
	}
	_, qr, err := vh.vtg.Execute(ctx, mysqlCtx, session, prepare.PrepareStmt, prepare.BindVars, true)
	if err != nil {
		return sqlerror.NewSQLErrorFromError(err)
	}
	fillInTxStatusFlags(c, session)

	return callback(qr)
}

func deferFirstOKOnlyResult(callback func(*sqltypes.Result) error) (func(*sqltypes.Result) error, func() *sqltypes.Result) {
	firstPacket := true
	var deferredResult *sqltypes.Result

	streamCallback := func(result *sqltypes.Result) error {
		if firstPacket {
			firstPacket = false
			if len(result.Fields) == 0 {
				deferredResult = result
				return nil
			}
		}
		return callback(result)
	}
	return streamCallback, func() *sqltypes.Result {
		return deferredResult
	}
}

func (vh *vtgateHandler) WarningCount(c *mysql.Conn) uint16 {
	return uint16(len(vh.session(c).GetWarnings()))
}

// ComRegisterReplica is part of the mysql.Handler interface.
func (vh *vtgateHandler) ComRegisterReplica(c *mysql.Conn, replicaHost string, replicaPort uint16, replicaUser string, replicaPassword string) error {
	return vterrors.VT12001("ComRegisterReplica for the VTGate handler")
}

// ComBinlogDump is part of the mysql.Handler interface.
// COM_BINLOG_DUMP (file/position-based) is not supported; clients should use COM_BINLOG_DUMP_GTID instead.
func (vh *vtgateHandler) ComBinlogDump(c *mysql.Conn, logFile string, binlogPos uint32) error {
	return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED,
		"COM_BINLOG_DUMP is not supported; use COM_BINLOG_DUMP_GTID instead")
}

// ComBinlogDumpGTID is part of the mysql.Handler interface.
// It handles binlog dump requests by forwarding them to a targeted vttablet.
// The target tablet is determined from the session's TargetString, which can be set via:
// 1. A USE statement (e.g., "USE `keyspace:shard@type`"), or
// 2. The username during connection (format: "user|keyspace:shard@type")
// Supported target formats:
//   - "keyspace:shard" (e.g., "commerce:0") — routes via health check, defaults to primary
//   - "keyspace:shard@type" (e.g., "commerce:-80@primary") — routes via health check
//   - "keyspace:shard@type|alias" (e.g., "commerce:-80@primary|zone1-100") — routes to specific tablet
func (vh *vtgateHandler) ComBinlogDumpGTID(c *mysql.Conn, logFile string, logPos uint64, gtidSet replication.GTIDSet, flags uint16) error {
	// Check for shutdown before starting a long-lived stream
	if c.IsShuttingDown() {
		c.MarkForClose()
		return sqlerror.NewSQLError(sqlerror.ERServerShutdown, sqlerror.SSNetError, "Server shutdown in progress")
	}

	// Track this connection as busy for graceful shutdown
	vh.busyConnections.Add(1)
	defer vh.busyConnections.Add(-1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.UpdateCancelCtx(cancel)

	// Add call info for observability
	ctx = callinfo.MysqlCallInfo(ctx, c)

	// Fill in the ImmediateCallerID with the UserData returned by
	// the AuthServer plugin for that user. If nothing was
	// returned, use the User. This lets the plugin map a MySQL
	// user used for authentication to a Vitess User used for
	// Table ACLs and Vitess authentication in general.
	im := c.UserData.Get()
	ef := callerid.NewEffectiveCallerID(
		c.User,                  /* principal: who */
		c.RemoteAddr().String(), /* component: running client process */
		"VTGate MySQL Connector" /* subcomponent: part of the client */)
	ctx = callerid.NewContext(ctx, ef, im)

	// Check if binlog dump is enabled globally
	if !enableBinlogDump.Get() {
		binlogDumpRequests.Add("disabled", 1)
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "binlog dump is disabled")
	}

	// Check user authorization for binlog dump
	if !binlogacl.Authorized(im) {
		binlogDumpRequests.Add("denied", 1)
		return vterrors.NewErrorf(vtrpcpb.Code_PERMISSION_DENIED, vterrors.AccessDeniedError, "User '%s' is not authorized to perform binlog dump operations", im.GetUsername())
	}

	binlogDumpRequests.Add("authorized", 1)

	// Get the target from the session (set by USE statement or parsed from username during handshake)
	session := vh.session(c)
	targetString := session.TargetString

	if targetString == "" {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "no target specified for binlog dump; use 'USE keyspace:shard@type' or connect with username 'user|keyspace:shard@type'")
	}

	// Parse the target string to extract the tablet alias
	keyspace, tabletType, dest, tabletAlias, err := topoproto.ParseDestination(targetString, topodatapb.TabletType_UNKNOWN)
	if err != nil {
		return vterrors.Wrapf(err, "failed to parse target: %s", targetString)
	}

	// Build the target for the tablet connection
	var target *querypb.Target
	if keyspace != "" {
		// Default to PRIMARY for binlog dump when no tablet type is specified
		if tabletType == topodatapb.TabletType_UNKNOWN {
			tabletType = topodatapb.TabletType_PRIMARY
		}
		target = &querypb.Target{
			Keyspace:   keyspace,
			TabletType: tabletType,
		}
		if dest != nil {
			// Extract shard from destination - need to type assert to get the raw shard name
			if ds, ok := dest.(key.DestinationShard); ok {
				target.Shard = string(ds)
			} else {
				return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "binlog dump requires a specific shard, got: %s", dest.String())
			}
		}
	}

	// Validate that at minimum keyspace and shard are specified
	if target == nil || target.Keyspace == "" || target.Shard == "" {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "binlog dump requires keyspace and shard (e.g., 'commerce:0', 'commerce:0@primary', 'commerce:0@primary|zone1-100'): %s", targetString)
	}

	// File/position-based replication is not supported through vtgate.
	// Binlog filenames and positions are local to individual MySQL instances and
	// differ across replicas, making them unsuitable for vtgate's routing model.
	// Use GTIDs for all binlog dump operations.
	if logFile != "" {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"binlog filename is not supported; use GTIDs instead")
	}
	if logPos < 4 {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"Client requested source to start replication from position < 4")
	}
	if logPos > 4 {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"only binlog position 4 is supported; use GTIDs for positioning")
	}

	// Build the BinlogDumpGTID request
	request := &binlogdatapb.BinlogDumpGTIDRequest{
		BinlogPosition: logPos,
		Flags:          uint32(flags),
		Target:         target,
	}
	if gtidSet != nil {
		request.GtidSet = gtidSet.String()
	}

	// TODO: Add support for replication session variables (for Fivetran MySQL adapter compatibility):
	// - @master_heartbeat_period / @source_heartbeat_period: Controls heartbeat frequency
	// - @master_binlog_checksum / @source_binlog_checksum: Controls checksum algorithm
	// Implementation requires:
	// 1. Add heartbeat_period_ns and binlog_checksum fields to BinlogDumpGTIDRequest proto
	// 2. Extract user-defined variables from session.UserDefinedVariables here
	// 3. Apply variables in vttablet's BinlogDump before sending COM_BINLOG_DUMP_GTID
	// See: https://dev.mysql.com/doc/refman/8.0/en/replication-options-replica.html

	var state binlogStreamState
	callback := vh.binlogStreamCallback(c, &state)

	if tabletAlias != nil {
		// Route to a specific tablet by alias
		qs, err := vh.vtg.Gateway().QueryServiceByAlias(ctx, tabletAlias, target)
		if err != nil {
			return vh.streamBinlogDumpResponse(c, "ComBinlogDumpGTID", &state, func() error {
				return vterrors.Wrapf(err, "failed to get connection to tablet %s", topoproto.TabletAliasString(tabletAlias))
			})
		}
		return vh.streamBinlogDumpResponse(c, "ComBinlogDumpGTID", &state, func() error {
			return qs.BinlogDumpGTID(ctx, request, callback)
		})
	}

	// Route via health check — gateway selects a healthy tablet for the target
	return vh.streamBinlogDumpResponse(c, "ComBinlogDumpGTID", &state, func() error {
		return vh.vtg.Gateway().BinlogDumpGTID(ctx, request, callback)
	})
}

// binlogStreamState tracks the state of a binlog dump stream for error handling.
type binlogStreamState struct {
	// streamingStarted is true once the first callback has been invoked.
	streamingStarted bool
	// inProgressMessage is true when the last packet written was exactly MaxPacketSize,
	// meaning a multi-packet message is in progress and the client expects more data.
	inProgressMessage bool
}

// binlogStreamCallback returns a streaming callback for binlog dump responses that handles
// packet spanning. The tablet-side streamBinlogPackets packs data into 256KB chunks, so
// individual MySQL packets may span multiple gRPC responses. This callback writes packet
// data directly to the client connection as it arrives, without buffering entire packets.
func (vh *vtgateHandler) binlogStreamCallback(c *mysql.Conn, state *binlogStreamState) func(*binlogdatapb.BinlogDumpResponse) error {
	// Spanning-packet state: when a MySQL packet spans multiple gRPC
	// responses, we stream the payload directly to the connection as
	// each chunk arrives. Only packetLength and written are needed.
	var packetLength int // total expected payload length of the spanning packet
	var written int      // bytes written so far for the spanning packet

	return func(response *binlogdatapb.BinlogDumpResponse) error {
		state.streamingStarted = true

		buf := response.Raw
		bufOffset := 0

		if packetLength > 0 {
			// We're in the middle of streaming a packet that spans multiple responses.
			remaining := packetLength - written
			if len(buf) < remaining {
				// This response doesn't have enough data to complete the packet.
				if err := c.WritePacketRaw(buf); err != nil {
					return err
				}
				written += len(buf)
				return c.FlushWriteBuffer()
			}

			// This response completes the spanning packet.
			if err := c.WritePacketRaw(buf[:remaining]); err != nil {
				return err
			}
			bufOffset = remaining
			packetLength = 0
			written = 0
			state.inProgressMessage = false
		}

		for len(buf)-bufOffset > 0 {
			if len(buf[bufOffset:]) < mysql.PacketHeaderSize {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "binlog dump: packet too small: %d bytes", len(buf))
			}

			header := buf[bufOffset : bufOffset+mysql.PacketHeaderSize]
			bufOffset += mysql.PacketHeaderSize

			pktLen := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

			state.inProgressMessage = pktLen == mysql.MaxPacketSize

			if pktLen <= len(buf[bufOffset:]) {
				// Common case: full packet fits in this response.
				if err := c.WritePacketDirect(buf[bufOffset : bufOffset+pktLen]); err != nil {
					return err
				}
				bufOffset += pktLen
			} else {
				// Packet spans multiple responses — write header and first chunk directly.
				packetLength = pktLen
				state.inProgressMessage = true
				if err := c.WritePacketHeader(pktLen); err != nil {
					return err
				}
				if err := c.WritePacketRaw(buf[bufOffset:]); err != nil {
					return err
				}
				written = len(buf) - bufOffset
				bufOffset = len(buf)
			}
		}

		return c.FlushWriteBuffer()
	}
}

// streamBinlogDumpResponse runs a binlog dump stream and handles error reporting.
// The streamFn should invoke the appropriate BinlogDump or BinlogDumpGTID RPC with
// a callback created by binlogStreamCallback that shares the given state.
// If an error occurs before streaming starts, it is returned to the handler framework.
// If an error occurs mid-message (after sending a max-size packet fragment),
// the connection is closed since we can't send a clean error packet.
func (vh *vtgateHandler) streamBinlogDumpResponse(c *mysql.Conn, caller string, state *binlogStreamState, streamFn func() error) error {
	err := streamFn()
	if err == nil {
		return nil
	}

	// If streaming never started, return the error normally so the
	// handler framework can send a proper error packet to the client.
	if !state.streamingStarted {
		return vterrors.Wrapf(err, "binlog dump failed")
	}

	// Streaming started. We need to handle the error carefully.
	if state.inProgressMessage {
		// We're mid-message (sent a max-size fragment). We can't send
		// a clean error packet since the client is expecting more data.
		// Just close the connection.
		c.MarkForClose()
		log.Error(fmt.Sprintf("%s: error mid-packet, closing connection: %v", caller, err))
		return nil
	}

	// At a message boundary - we can send a proper error packet.
	if writeErr := c.WriteErrorPacketFromError(err); writeErr != nil {
		log.Error(fmt.Sprintf("%s: failed to write error packet: %v", caller, writeErr))
	}
	c.MarkForClose()
	log.Error(fmt.Sprintf("%s: %v", caller, err))
	return nil
}

// KillConnection closes an open connection by connection ID.
func (vh *vtgateHandler) KillConnection(ctx context.Context, connectionID uint32) error {
	vh.mu.Lock()
	defer vh.mu.Unlock()

	c, exists := vh.connections[connectionID]
	if !exists {
		return sqlerror.NewSQLErrorf(sqlerror.ERNoSuchThread, sqlerror.SSUnknownSQLState, "Unknown thread id: %d", connectionID)
	}

	// First, we mark the connection for close, so that even when the context is cancelled, while returning the response back to client,
	// the connection can get closed,
	// Closing the connection will trigger ConnectionClosed method which rollback any open transaction.
	c.MarkForClose()
	c.CancelCtx()

	return nil
}

// KillQuery cancels any execution query on the provided connection ID.
func (vh *vtgateHandler) KillQuery(connectionID uint32) error {
	vh.mu.Lock()
	defer vh.mu.Unlock()
	c, exists := vh.connections[connectionID]
	if !exists {
		return sqlerror.NewSQLErrorf(sqlerror.ERNoSuchThread, sqlerror.SSUnknownSQLState, "Unknown thread id: %d", connectionID)
	}
	c.CancelCtx()
	return nil
}

func (vh *vtgateHandler) Env() *vtenv.Environment {
	return vh.vtg.executor.env
}

func (vh *vtgateHandler) session(c *mysql.Conn) *vtgatepb.Session {
	session, _ := c.ClientData.(*vtgatepb.Session)
	if session == nil {
		u, _ := uuid.NewUUID()
		session = &vtgatepb.Session{
			Options: &querypb.ExecuteOptions{
				IncludedFields: querypb.ExecuteOptions_ALL,
				Workload:       querypb.ExecuteOptions_Workload(mysqlDefaultWorkload),

				// The collation field of ExecuteOption is set right before an execution.
			},
			Autocommit:           true,
			DDLStrategy:          defaultDDLStrategy,
			MigrationContext:     "",
			SessionUUID:          u.String(),
			EnableSystemSettings: sysVarSetEnabled,
		}
		if c.Capabilities&mysql.CapabilityClientFoundRows != 0 {
			session.Options.ClientFoundRows = true
		}
		c.ClientData = session
	}
	return session
}

type mysqlServer struct {
	tcpListener     *mysql.Listener
	unixListener    *mysql.Listener
	sigChan         chan os.Signal
	vtgateHandle    *vtgateHandler
	heartbeatCancel context.CancelFunc
}

// initTLSConfig inits tls config for the given mysql listener
func initTLSConfig(ctx context.Context, srv *mysqlServer, mysqlSslCert, mysqlSslKey, mysqlSslCa, mysqlSslCrl, mysqlSslServerCA string, mysqlServerRequireSecureTransport bool, mysqlMinTLSVersion uint16) error {
	serverConfig, err := vttls.ServerConfig(mysqlSslCert, mysqlSslKey, mysqlSslCa, mysqlSslCrl, mysqlSslServerCA, mysqlMinTLSVersion)
	if err != nil {
		log.Error(fmt.Sprintf("grpcutils.TLSServerConfig failed: %v", err))
		os.Exit(1)
		return err
	}
	srv.tcpListener.TLSConfig.Store(serverConfig)
	srv.tcpListener.RequireSecureTransport = mysqlServerRequireSecureTransport
	srv.sigChan = make(chan os.Signal, 1)
	signal.Notify(srv.sigChan, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-srv.sigChan:
				serverConfig, err := vttls.ServerConfig(mysqlSslCert, mysqlSslKey, mysqlSslCa, mysqlSslCrl, mysqlSslServerCA, mysqlMinTLSVersion)
				if err != nil {
					log.Error(fmt.Sprintf("grpcutils.TLSServerConfig failed: %v", err))
				} else {
					log.Info("grpcutils.TLSServerConfig updated")
					srv.tcpListener.TLSConfig.Store(serverConfig)
				}
			}
		}
	}()
	return nil
}

// initMySQLProtocol starts the mysql protocol.
// It should be called only once in a process.
func initMySQLProtocol(vtgate *VTGate) *mysqlServer {
	// Flag is not set, just return.
	if mysqlServerPort < 0 && mysqlServerSocketPath == "" {
		return nil
	}

	// If no VTGate was created, just return.
	if vtgate == nil {
		return nil
	}

	// Initialize registered AuthServer implementations (or other plugins)
	for _, initFn := range pluginInitializers {
		initFn()
	}
	authServer := mysql.GetAuthServer(mysqlAuthServerImpl)

	// Check mysql-default-workload
	var ok bool
	if mysqlDefaultWorkload, ok = querypb.ExecuteOptions_Workload_value[strings.ToUpper(mysqlDefaultWorkloadName)]; !ok {
		log.Error("-mysql-default-workload must be one of [OLTP, OLAP, DBA, UNSPECIFIED]")
		os.Exit(1)
	}

	switch mysqlTCPVersion {
	case "tcp", "tcp4", "tcp6":
		// Valid flag value.
	default:
		log.Error("-mysql-tcp-version must be one of [tcp, tcp4, tcp6]")
		os.Exit(1)
	}

	// Create a Listener.
	var err error
	srv := &mysqlServer{}
	srv.vtgateHandle = newVtgateHandler(vtgate)

	// Keep reserved connections that hold temporary tables alive with a
	// low-frequency background heartbeat. Cancelled at shutdown.
	var heartbeatCtx context.Context
	heartbeatCtx, srv.heartbeatCancel = context.WithCancel(context.Background())
	srv.vtgateHandle.startTempTableHeartbeat(heartbeatCtx)

	if mysqlServerPort >= 0 {
		listener, err := servenv.Listen(mysqlTCPVersion, net.JoinHostPort(mysqlServerBindAddress, strconv.Itoa(mysqlServerPort)))
		if err != nil {
			log.Error(fmt.Sprintf("servenv.Listen failed: %v", err))
			os.Exit(1)
		}
		srv.tcpListener, err = mysql.NewFromListener(
			listener,
			authServer,
			srv.vtgateHandle,
			mysqlConnReadTimeout,
			mysqlConnWriteTimeout,
			mysqlProxyProtocol,
			mysqlConnBufferPooling,
			mysqlKeepAlivePeriod,
			mysqlServerFlushDelay,
			mysqlServerMultiQuery,
		)
		if err != nil {
			log.Error(fmt.Sprintf("mysql.NewFromListener failed: %v", err))
			os.Exit(1)
		}
		if mysqlSslCert != "" && mysqlSslKey != "" {
			tlsVersion, err := vttls.TLSVersionToNumber(mysqlTLSMinVersion)
			if err != nil {
				log.Error(fmt.Sprintf("mysql.NewFromListener failed: %v", err))
				os.Exit(1)
			}

			_ = initTLSConfig(context.Background(), srv, mysqlSslCert, mysqlSslKey, mysqlSslCa, mysqlSslCrl, mysqlSslServerCA, mysqlServerRequireSecureTransport, tlsVersion)
		}
		srv.tcpListener.AllowClearTextWithoutTLS.Store(mysqlAllowClearTextWithoutTLS)
		// Check for the connection threshold
		if mysqlSlowConnectWarnThreshold != 0 {
			log.Info(fmt.Sprintf("setting mysql slow connection threshold to %v", mysqlSlowConnectWarnThreshold))
			srv.tcpListener.SlowConnectWarnThreshold.Store(mysqlSlowConnectWarnThreshold.Nanoseconds())
		}
		// Start listening for tcp
		go srv.tcpListener.Accept()
	}

	if mysqlServerSocketPath != "" {
		err = setupUnixSocket(srv, authServer, mysqlServerSocketPath)
		if err != nil {
			log.Error(fmt.Sprintf("mysql.NewListener failed: %v", err))
			os.Exit(1)
		}
	}
	return srv
}

// newMysqlUnixSocket creates a new unix socket mysql listener. If a socket file already exists, attempts
// to clean it up.
func newMysqlUnixSocket(address string, authServer mysql.AuthServer, handler mysql.Handler) (*mysql.Listener, error) {
	listener, err := mysql.NewListener(
		"unix",
		address,
		authServer,
		handler,
		mysqlConnReadTimeout,
		mysqlConnWriteTimeout,
		false,
		mysqlConnBufferPooling,
		mysqlKeepAlivePeriod,
		mysqlServerFlushDelay,
		mysqlServerMultiQuery,
	)

	switch err := err.(type) {
	case nil:
		return listener, nil
	case *net.OpError:
		log.Warn(fmt.Sprintf("Found existent socket when trying to create new unix mysql listener: %s, attempting to clean up", address))
		// err.Op should never be different from listen, just being extra careful
		// in case in the future other errors are returned here
		if err.Op != "listen" {
			return nil, err
		}
		_, dialErr := net.Dial("unix", address)
		if dialErr == nil {
			log.Error(fmt.Sprintf("Existent socket '%s' is still accepting connections, aborting", address))
			return nil, err
		}
		removeFileErr := os.Remove(address)
		if removeFileErr != nil {
			log.Error("Couldn't remove existent socket file: " + address)
			return nil, err
		}
		listener, listenerErr := mysql.NewListener(
			"unix",
			address,
			authServer,
			handler,
			mysqlConnReadTimeout,
			mysqlConnWriteTimeout,
			false,
			mysqlConnBufferPooling,
			mysqlKeepAlivePeriod,
			mysqlServerFlushDelay,
			mysqlServerMultiQuery,
		)
		return listener, listenerErr
	default:
		return nil, err
	}
}

func (srv *mysqlServer) shutdownMysqlProtocolAndDrain() {
	if srv.heartbeatCancel != nil {
		srv.heartbeatCancel()
	}
	if srv.sigChan != nil {
		signal.Stop(srv.sigChan)
	}
	setListenerToNil := func() {
		srv.tcpListener = nil
		srv.unixListener = nil
	}

	if mysqlDrainOnTerm {
		stopListener(srv.unixListener, false)
		stopListener(srv.tcpListener, false)
		setListenerToNil()
		// We wait for connected clients to drain by themselves or to run into the onterm timeout
		log.Info("Starting drain loop, waiting for all clients to disconnect")
		reported := time.Now()
		for srv.vtgateHandle.numConnections() > 0 {
			if time.Since(reported) > 2*time.Second {
				log.Info(fmt.Sprintf("Still waiting for client connections to drain (%d connected)...", srv.vtgateHandle.numConnections()))
				reported = time.Now()
			}
			time.Sleep(1000 * time.Millisecond)
		}
		return
	}

	stopListener(srv.unixListener, true)
	stopListener(srv.tcpListener, true)
	setListenerToNil()
	if busy := srv.vtgateHandle.busyConnections.Load(); busy > 0 {
		log.Info(fmt.Sprintf("Waiting for all client connections to be idle (%d active)...", busy))
		start := time.Now()
		reported := start
		for busy > 0 {
			if time.Since(reported) > 2*time.Second {
				log.Info(fmt.Sprintf("Still waiting for client connections to be idle (%d active)...", busy))
				reported = time.Now()
			}

			time.Sleep(1 * time.Millisecond)
			busy = srv.vtgateHandle.busyConnections.Load()
		}
	}
}

// stopListener Close or Shutdown a mysql listener depending on the shutdown argument.
func stopListener(listener *mysql.Listener, shutdown bool) {
	if listener == nil {
		return
	}
	if shutdown {
		listener.Shutdown()
	} else {
		listener.Close()
	}
}

func (srv *mysqlServer) rollbackAtShutdown() {
	defer log.Flush()
	if srv.vtgateHandle == nil {
		// we still haven't been able to initialise the vtgateHandler, so we don't need to rollback anything
		return
	}

	// Close all open connections. If they're waiting for reads, this will cause
	// them to error out, which will automatically rollback open transactions.
	func() {
		if srv.vtgateHandle != nil {
			srv.vtgateHandle.mu.Lock()
			defer srv.vtgateHandle.mu.Unlock()
			for id, c := range srv.vtgateHandle.connections {
				if c != nil {
					log.Info(fmt.Sprintf("Rolling back transactions associated with connection ID: %v", id))
					c.Close()
				}
			}
		}
	}()

	// If vtgate is instead busy executing a query, the number of open conns
	// will be non-zero. Give another second for those queries to finish.
	for range 100 {
		if srv.vtgateHandle.numConnections() == 0 {
			log.Info("All connections have been rolled back.")
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	log.Error("All connections did not go idle. Shutting down anyway.")
}

func mysqlSocketPath() string {
	if mysqlServerSocketPath == "" {
		return ""
	}
	return mysqlServerSocketPath
}

// GetMysqlServerSSLCA returns the current value of the mysql-server-ssl-ca flag
func GetMysqlServerSSLCA() string {
	return mysqlSslCa
}

func init() {
	servenv.OnParseFor("vtgate", registerPluginFlags)
	servenv.OnParseFor("vtcombo", registerPluginFlags)
}

var pluginInitializers []func()

// RegisterPluginInitializer lets plugins register themselves to be init'ed at servenv.OnRun-time
func RegisterPluginInitializer(initializer func()) {
	pluginInitializers = append(pluginInitializers, initializer)
}
