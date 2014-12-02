// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"code.google.com/p/go.net/context"
	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/acl"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/streamlog"
	"github.com/youtube/vitess/go/vt/cfgclient"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

var (
	queryLogHandler = flag.String("query-log-stream-handler", "/debug/querylog", "URL handler for streaming queries log")
	txLogHandler    = flag.String("transaction-log-stream-handler", "/debug/txlog", "URL handler for streaming transactions log")

	// Currently only local custom rule files are loaded to build custom query rules
	// Google internally use CDD to push custom rules to tabletservers. For this purpose,
	// Zookeeper can used instead of CDD as a opensource solution
	customRules = flag.String("customrules", "", "custom query rules source")
)

func init() {
	flag.IntVar(&qsConfig.PoolSize, "queryserver-config-pool-size", DefaultQsConfig.PoolSize, "query server pool size")
	flag.IntVar(&qsConfig.StreamPoolSize, "queryserver-config-stream-pool-size", DefaultQsConfig.StreamPoolSize, "query server stream pool size")
	flag.IntVar(&qsConfig.TransactionCap, "queryserver-config-transaction-cap", DefaultQsConfig.TransactionCap, "query server transaction cap")
	flag.Float64Var(&qsConfig.TransactionTimeout, "queryserver-config-transaction-timeout", DefaultQsConfig.TransactionTimeout, "query server transaction timeout")
	flag.IntVar(&qsConfig.MaxResultSize, "queryserver-config-max-result-size", DefaultQsConfig.MaxResultSize, "query server max result size")
	flag.IntVar(&qsConfig.MaxDMLRows, "queryserver-config-max-dml-rows", DefaultQsConfig.MaxDMLRows, "query server max dml rows per statement")
	flag.IntVar(&qsConfig.StreamBufferSize, "queryserver-config-stream-buffer-size", DefaultQsConfig.StreamBufferSize, "query server stream buffer size")
	flag.IntVar(&qsConfig.QueryCacheSize, "queryserver-config-query-cache-size", DefaultQsConfig.QueryCacheSize, "query server query cache size")
	flag.Float64Var(&qsConfig.SchemaReloadTime, "queryserver-config-schema-reload-time", DefaultQsConfig.SchemaReloadTime, "query server schema reload time")
	flag.Float64Var(&qsConfig.QueryTimeout, "queryserver-config-query-timeout", DefaultQsConfig.QueryTimeout, "query server query timeout")
	flag.Float64Var(&qsConfig.TxPoolTimeout, "queryserver-config-txpool-timeout", DefaultQsConfig.TxPoolTimeout, "query server transaction pool timeout")
	flag.Float64Var(&qsConfig.IdleTimeout, "queryserver-config-idle-timeout", DefaultQsConfig.IdleTimeout, "query server idle timeout")
	flag.Float64Var(&qsConfig.SpotCheckRatio, "queryserver-config-spot-check-ratio", DefaultQsConfig.SpotCheckRatio, "query server rowcache spot check frequency")
	flag.BoolVar(&qsConfig.StrictMode, "queryserver-config-strict-mode", DefaultQsConfig.StrictMode, "allow only predictable DMLs and enforces MySQL's STRICT_TRANS_TABLES")
	flag.BoolVar(&qsConfig.StrictTableAcl, "queryserver-config-strict-table-acl", DefaultQsConfig.StrictTableAcl, "only allow queries that pass table acl checks")
	flag.StringVar(&qsConfig.RowCache.Binary, "rowcache-bin", DefaultQsConfig.RowCache.Binary, "rowcache binary file")
	flag.IntVar(&qsConfig.RowCache.Memory, "rowcache-memory", DefaultQsConfig.RowCache.Memory, "rowcache max memory usage in MB")
	flag.StringVar(&qsConfig.RowCache.Socket, "rowcache-socket", DefaultQsConfig.RowCache.Socket, "rowcache socket path to listen on")
	flag.IntVar(&qsConfig.RowCache.TcpPort, "rowcache-port", DefaultQsConfig.RowCache.TcpPort, "rowcache tcp port to listen on")
	flag.IntVar(&qsConfig.RowCache.Connections, "rowcache-connections", DefaultQsConfig.RowCache.Connections, "rowcache max simultaneous connections")
	flag.IntVar(&qsConfig.RowCache.Threads, "rowcache-threads", DefaultQsConfig.RowCache.Threads, "rowcache number of threads")
	flag.BoolVar(&qsConfig.RowCache.LockPaged, "rowcache-lock-paged", DefaultQsConfig.RowCache.LockPaged, "whether rowcache locks down paged memory")

	var fcr FileCustomRule
	customRuleClientImpls["file"] = &fcr
}

type RowCacheConfig struct {
	Binary      string
	Memory      int
	Socket      string
	TcpPort     int
	Connections int
	Threads     int
	LockPaged   bool
}

func (c *RowCacheConfig) GetSubprocessFlags() []string {
	cmd := []string{}
	if c.Binary == "" {
		return cmd
	}
	cmd = append(cmd, c.Binary)
	if c.Memory > 0 {
		// memory is given in bytes and rowcache expects in MBs
		cmd = append(cmd, "-m", strconv.Itoa(c.Memory/1000000))
	}
	if c.Socket != "" {
		cmd = append(cmd, "-s", c.Socket)
	}
	if c.TcpPort > 0 {
		cmd = append(cmd, "-p", strconv.Itoa(c.TcpPort))
	}
	if c.Connections > 0 {
		cmd = append(cmd, "-c", strconv.Itoa(c.Connections))
	}
	if c.Threads > 0 {
		cmd = append(cmd, "-t", strconv.Itoa(c.Threads))
	}
	if c.LockPaged {
		cmd = append(cmd, "-k")
	}
	return cmd
}

type Config struct {
	PoolSize           int
	StreamPoolSize     int
	TransactionCap     int
	TransactionTimeout float64
	MaxResultSize      int
	MaxDMLRows         int
	StreamBufferSize   int
	QueryCacheSize     int
	SchemaReloadTime   float64
	QueryTimeout       float64
	TxPoolTimeout      float64
	IdleTimeout        float64
	RowCache           RowCacheConfig
	SpotCheckRatio     float64
	StrictMode         bool
	StrictTableAcl     bool
}

// DefaultQSConfig is the default value for the query service config.
//
// The value for StreamBufferSize was chosen after trying out a few of
// them. Too small buffers force too many packets to be sent. Too big
// buffers force the clients to read them in multiple chunks and make
// memory copies.  so with the encoding overhead, this seems to work
// great (the overhead makes the final packets on the wire about twice
// bigger than this).
var DefaultQsConfig = Config{
	PoolSize:           16,
	StreamPoolSize:     750,
	TransactionCap:     20,
	TransactionTimeout: 30,
	MaxResultSize:      10000,
	MaxDMLRows:         500,
	QueryCacheSize:     5000,
	SchemaReloadTime:   30 * 60,
	QueryTimeout:       0,
	TxPoolTimeout:      1,
	IdleTimeout:        30 * 60,
	StreamBufferSize:   32 * 1024,
	RowCache:           RowCacheConfig{Memory: -1, TcpPort: -1, Connections: -1, Threads: -1},
	SpotCheckRatio:     0,
	StrictMode:         true,
	StrictTableAcl:     false,
}

var qsConfig Config

var SqlQueryRpcService *SqlQuery

// registration service for all server protocols

type SqlQueryRegisterFunction func(*SqlQuery)

var SqlQueryRegisterFunctions []SqlQueryRegisterFunction

func RegisterQueryService() {
	if SqlQueryRpcService != nil {
		log.Warningf("RPC service already up %v", SqlQueryRpcService)
		return
	}
	SqlQueryRpcService = NewSqlQuery(qsConfig)
	for _, f := range SqlQueryRegisterFunctions {
		f(SqlQueryRpcService)
	}
	http.HandleFunc("/debug/health", healthCheck)
}

// AllowQueries can take an indefinite amount of time to return because
// it keeps retrying until it obtains a valid connection to the database.
func AllowQueries(dbconfigs *dbconfigs.DBConfigs, schemaOverrides []SchemaOverride, qrs *QueryRules, mysqld *mysqlctl.Mysqld, waitForMysql bool) error {
	return SqlQueryRpcService.allowQueries(dbconfigs, schemaOverrides, qrs, mysqld, waitForMysql)
}

// DisallowQueries can take a long time to return (not indefinite) because
// it has to wait for queries & transactions to be completed or killed,
// and also for house keeping goroutines to be terminated.
func DisallowQueries() {
	defer logError()
	SqlQueryRpcService.disallowQueries()
}

// Reload the schema. If the query service is not running, nothing will happen
func ReloadSchema() {
	defer logError()
	SqlQueryRpcService.qe.schemaInfo.triggerReload()
}

func GetSessionId() int64 {
	return SqlQueryRpcService.sessionId
}

func SetQueryRules(qrs *QueryRules) {
	SqlQueryRpcService.qe.schemaInfo.SetRules(qrs)
}

func GetQueryRules() (qrs *QueryRules) {
	return SqlQueryRpcService.qe.schemaInfo.GetRules()
}

// IsHealthy returns nil if the query service is healthy (able to
// connect to the database and serving traffic) or an error explaining
// the unhealthiness otherwise.
func IsHealthy() error {
	return SqlQueryRpcService.Execute(
		context.Background(),
		&proto.Query{Sql: "select 1 from dual", SessionId: SqlQueryRpcService.sessionId},
		new(mproto.QueryResult),
	)
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.MONITORING); err != nil {
		acl.SendError(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	if err := IsHealthy(); err != nil {
		w.Write([]byte("not ok"))
		return
	}
	w.Write([]byte("ok"))
}

func buildFmter(logger *streamlog.StreamLogger) func(url.Values, interface{}) string {
	type formatter interface {
		Format(url.Values) string
	}

	return func(params url.Values, val interface{}) string {
		fmter, ok := val.(formatter)
		if !ok {
			return fmt.Sprintf("Error: unexpected value of type %T in %s!", val, logger.Name())
		}
		return fmter.Format(params)
	}
}

// InitQueryService registers the query service, after loading any
// necessary config files. It also starts any relevant streaming logs.
func InitQueryService() {
	SqlQueryLogger.ServeLogs(*queryLogHandler, buildFmter(SqlQueryLogger))
	TxLogger.ServeLogs(*txLogHandler, buildFmter(TxLogger))
	RegisterQueryService()
}

// CustomRuleClient inherits Cfgclient and provide BuildQueryRules()
// interface which converts raw byte of certain format (JSON, ProtoBuf...)
// into QueryRules structure.
type CustomRuleClient interface {
	cfgclient.CfgClient
	BuildQueryRules([]byte) (*QueryRules, error)
}

// Registered custom rule implementations
var customRuleClientImpls map[string](CustomRuleClient) = make(map[string](CustomRuleClient))

// The actual custom rule client implementation in use is determined by flag 'customrules'.
// The flag 'customrules' follows a format of <Custom Rule Implementation>:<PATH>, for example
// If customrules=file:/usr/local/data/somerule.rule or customrules=/usr/local/data/rule1.rule,
// the "file" custom rule client will be used to build custom query rules

var customRuleClientSelected string = "file"

type FileCustomRule struct {
	FilePath string
}

// So a filesystem normally cannot actively notify modifications
func (fcr *FileCustomRule) UpdateCfg(data []byte) (err error) {
	return nil
}

func (fcr *FileCustomRule) GetCfg() (content []byte, err error) {
	content, err = ioutil.ReadFile(fcr.FilePath)
	return content, err
}

func (fcr *FileCustomRule) Init(params map[string](interface{})) (err error) {
	if path, havepath := params["path"]; havepath {
		switch pt := path.(type) {
		case *string:
			fcr.FilePath = *pt
			return nil
		default:
			return errors.New("Custom rule path needs to be of string type")
		}
	}
	return errors.New("No path parameter is found")
}

func (fcr *FileCustomRule) BuildQueryRules(rawtxt []byte) (qrs *QueryRules, err error) {
	qrs = NewQueryRules()
	err = qrs.UnmarshalJSON(rawtxt)
	return qrs, err
}

// LoadCustomRules returns custom rules as specified by the command
// line flags. LoadCustomRules() should be called after InitQueryService()
func LoadCustomRules() (qrs *QueryRules) {
	if *customRules == "" {
		return NewQueryRules()
	}

	// Determine which CustomRuleClient to use
	// Custom Rule paths is of the style <proto>:<path>
	if !strings.Contains(*customRules, ":") {
		*customRules = "file:" + *customRules
	}
	*customRules = *customRules + " "
	// Get the protocol to retrive custom rule
	ruleproto := strings.Split(*customRules, ":")[0]
	// Get the custom rule path under given protocol
	rulepath := strings.TrimSpace((*customRules)[(len(ruleproto) + 1):len(*customRules)])
	if _, ok := customRuleClientImpls[ruleproto]; !ok {
		return NewQueryRules()
	}
	// Get custom rule client & Initalization
	ruleclient := customRuleClientImpls[ruleproto]
	initparams := map[string](interface{}){"path": &rulepath, "sqlquery": SqlQueryRpcService}
	err := ruleclient.Init(initparams)
	if err != nil {
		log.Warningf("Cannot initialize custom rule client, err: %v\n", err)
	}

	//Read initial configuration and build QueryRules
	data, err := ruleclient.GetCfg()
	if err != nil {
		log.Warningf("Error fetching inital custom rule from %v: %v\n", *customRules, err)
	}
	qrs, err = ruleclient.BuildQueryRules(data)
	return qrs
}
