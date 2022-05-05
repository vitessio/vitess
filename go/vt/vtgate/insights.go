/*
Copyright 2022 The Vitess Authors.

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
	"crypto/tls"
	"flag"
	math2 "math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/vt/sqlparser"

	"google.golang.org/protobuf/encoding/prototext"

	"github.com/pkg/errors"

	"github.com/google/uuid"
	pbenvelope "github.com/planetscale/psevents/go/v1"
	pbvtgate "github.com/planetscale/psevents/go/vtgate/v1"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/math"
)

const (
	queryTopic            = "vtgate.v1.Query"
	queryStatsBundleTopic = "vtgate.v1.QueryStatsBundle"
	queryURLBase          = "psevents.planetscale.com"
	BrokersVar            = "INSIGHTS_KAFKA_BROKERS"
	UsernameVar           = "INSIGHTS_KAFKA_USERNAME"
	PasswordVar           = "INSIGHTS_KAFKA_PASSWORD"
)

type QueryPatternAggregation struct {
	QueryCount         uint64
	ErrorCount         uint64
	SumShardQueries    uint64
	MaxShardQueries    uint64
	SumRowsRead        uint64
	MaxRowsRead        uint64
	SumRowsAffected    uint64
	MaxRowsAffected    uint64
	SumRowsReturned    uint64
	MaxRowsReturned    uint64
	SumTotalDuration   time.Duration
	MaxTotalDuration   time.Duration
	SumPlanDuration    time.Duration
	MaxPlanDuration    time.Duration
	SumExecuteDuration time.Duration
	MaxExecuteDuration time.Duration
	SumCommitDuration  time.Duration
	MaxCommitDuration  time.Duration

	StatementType string
	Tables        []string
}

type QueryPatternKey struct {
	Keyspace string
	SQL      string
}

type Insights struct {
	// configuration
	DatabaseBranchPublicID string
	Brokers                []string
	Username               string
	Password               string
	MaxInFlight            uint
	Interval               time.Duration
	MaxPatterns            uint
	RowsReadThreshold      uint
	ResponseTimeThreshold  uint
	KafkaText              bool // use human-readable pb, for tests and debugging

	// state
	KafkaWriter     *kafka.Writer
	Aggregations    map[QueryPatternKey]*QueryPatternAggregation
	PeriodStart     time.Time
	InFlightCounter uint64
	Timer           *time.Ticker
	LogChan         chan interface{}
	Workers         sync.WaitGroup

	// log state: we limit some log messages to once per 15s because they're caused by behavior the
	// client controls
	LogPatternsExceeded uint
	LogBufferExceeded   uint

	// hooks
	Sender func([]byte, string, string) error
}

var (
	// insightsKafkaBrokers specifies a comma-separated list of host:port endpoints where Insights metrics are sent to Kafka.
	// If omitted (the default), no Insights metrics are sent.
	insightsKafkaBrokers = flag.String("insights_kafka_brokers", "", "Enable Kafka metrics to the given host:port endpoint")

	// insightsKafkaUsername specifies the username sent to authenticate to the Kafka endpoint
	insightsKafkaUsername = flag.String("insights_kafka_username", "", "Username for the Kafka endpoint")

	// insightsKafkaPassword specifies the password sent to authenticate to the Kafka endpoint
	insightsKafkaPassword = flag.String("insights_kafka_password", "", "Password for the Kafka endpoint")

	// insightsKafkaBufferSize in a cap on the message payload bytes that can be inflight (i.e. not yet sent to Kafka) before we start dropping messages to avoid unbounded memory usage if Kafka is down/slow
	insightsKafkaBufferSize = flag.Uint("insights_kafka_buffer", 5*1024*1024, "Maximum memory dedicated to unsent Kafka messages; above this threshold, messages will be dropped")

	// insightsRTThreshold is the response-time threshold in milliseconds, above which individual queries are reported
	insightsRTThreshold = flag.Uint("insights_rt_threshold", 1000, "Report individual queries that take at least this many milliseconds")

	// insightsRowsReadThreshold is the threshold on the number of rows read (scanned) above which individual queries are reported
	insightsRowsReadThreshold = flag.Uint("insights_rows_read_threshold", 1000, "Report individual transactions that read (scan) at least this many rows")

	// insightsFlushInterval is how often, in seconds, to send aggregated metrics for query patterns
	insightsFlushInterval = flag.Uint("insights_flush_interval", 15, "Send aggregated metrics for all query patterns every N seconds")

	// insightsPatternLimit is the maximum number of query patterns to track between flushes.  The first N patterns are tracked, and anything beyond
	// that is silently dropped until the next flush time.
	insightsPatternLimit = flag.Uint("insights_pattern_limit", 1000, "Maximum number of unique patterns to track in a flush interval")

	// databaseBranchPublicID is api-bb's name for the database branch this cluster hosts
	databaseBranchPublicID = flag.String("database_branch_public_id", "", "The public ID of the database branch this cluster hosts, used for Insights")

	// insightsKafkaText is true if we should send protobufs message in clear text, for unit tests and debugging
	insightsKafkaText = flag.Bool("insights_kafka_text", false, "Send Insights messages as plain text")
)

func initInsights(logger *streamlog.StreamLogger) (*Insights, error) {
	return initInsightsInner(logger,
		argOrEnv(*insightsKafkaBrokers, BrokersVar),
		*databaseBranchPublicID,
		argOrEnv(*insightsKafkaUsername, UsernameVar),
		argOrEnv(*insightsKafkaPassword, PasswordVar),
		*insightsKafkaBufferSize,
		*insightsPatternLimit,
		*insightsRowsReadThreshold,
		*insightsRTThreshold,
		time.Duration(*insightsFlushInterval)*time.Second,
		*insightsKafkaText,
	)
}

func initInsightsInner(logger *streamlog.StreamLogger, brokers, publicID, username, password string, bufsize, patternLimit, rowsReadThreshold, responseTimeThreshold uint, interval time.Duration, kafkaText bool) (*Insights, error) {
	if brokers == "" {
		return nil, nil
	}

	if publicID == "" {
		return nil, errors.New("-database_branch_public_id is required if Insights is enabled")
	}

	insights := Insights{
		DatabaseBranchPublicID: publicID,
		Brokers:                strings.Split(brokers, ","),
		Username:               username,
		Password:               password,
		MaxInFlight:            bufsize,
		Interval:               interval,
		MaxPatterns:            patternLimit,
		RowsReadThreshold:      rowsReadThreshold,
		ResponseTimeThreshold:  responseTimeThreshold,
		KafkaText:              kafkaText,
	}
	insights.Sender = insights.sendToKafka
	err := insights.logToKafka(logger)
	if err != nil {
		return nil, err
	}
	return &insights, nil
}

func (ii *Insights) Drain() bool {
	if ii.LogChan == nil {
		return true
	}

	close(ii.LogChan)
	ii.Workers.Wait()
	return ii.LogChan == nil
}

func argOrEnv(argVal, envKey string) string {
	if argVal != "" {
		return argVal
	}
	return os.Getenv(envKey)
}

func newPatternAggregation(statementType string, tables []string) *QueryPatternAggregation {
	return &QueryPatternAggregation{
		StatementType: statementType,
		Tables:        tables,
	}
}

func (ii *Insights) startInterval() {
	ii.Aggregations = make(map[QueryPatternKey]*QueryPatternAggregation)
	ii.PeriodStart = time.Now()
}

func (ii *Insights) shouldSendToInsights(ls *LogStats) bool {
	return ls.TotalTime().Milliseconds() > int64(ii.ResponseTimeThreshold) || ls.RowsRead >= uint64(ii.RowsReadThreshold) || ls.Error != nil
}

func (ii *Insights) logToKafka(logger *streamlog.StreamLogger) error {
	var transport kafka.RoundTripper

	if ii.Username != "" && ii.Password != "" {
		t := &kafka.Transport{
			TLS: &tls.Config{},
		}

		mechanism, err := scram.Mechanism(scram.SHA512, ii.Username, ii.Password)
		if err != nil {
			return errors.Wrap(err, "kafka scram configuration failed")
		}

		t.SASL = mechanism
		transport = t
	} else if ii.Username != "" {
		return errors.New("kafka username specified without a password")
	} else if ii.Password != "" {
		return errors.New("kafka password specified without a username")
	} else {
		transport = kafka.DefaultTransport
	}

	ii.KafkaWriter = &kafka.Writer{
		// This should be set to the bootstrap brokers (which the MSK interface provides)
		Addr:        kafka.TCP(ii.Brokers...),
		Balancer:    &kafka.Murmur2Balancer{},
		Transport:   transport,
		Compression: kafka.Lz4,
		// Setting acks=1, so that we lose fewer messages when partition leadership changes (which happens when a broker restarts, for example).
		// If we find ourselves unable to produce messages quickly enough, we can set this to kafka.None
		RequiredAcks: kafka.RequireOne,
		Async:        true,

		// The logger is pretty chatty (at least one log line per batch), so we shouldn't send this if filling up logs is a concern
		Logger:      kafka.LoggerFunc(log.Infof),
		ErrorLogger: kafka.LoggerFunc(log.Errorf),
		Completion: func(messages []kafka.Message, err error) {
			release := 0
			for _, m := range messages {
				release += len(m.Value)
			}
			atomic.AddUint64(&ii.InFlightCounter, -uint64(release))

			if err != nil {
				// log or increment failed send counter with len(messages)
				log.Warningf("Could not send %d-byte message to Kafka: %v", release, err)
			}
		},
	}

	ii.startInterval()

	ii.LogChan = logger.Subscribe("Kafka")
	ii.Workers.Add(1)
	ii.Timer = time.NewTicker(ii.Interval)
	go func() {
		defer func() {
			logger.Unsubscribe(ii.LogChan)
			ii.LogChan = nil
			ii.Timer.Stop()
			ii.Workers.Done()
		}()
		for {
			select {
			case record, ok := <-ii.LogChan:
				if !ok {
					// eof means someone called Drain to kill this worker
					return
				}
				if record == nil {
					// unit tests send a nil record to emulate a 15s heartbeat
					ii.sendAggregates()
				} else {
					ii.handleMessage(record)
				}
			case <-ii.Timer.C:
				ii.sendAggregates()
			}
		}
	}()

	return nil
}

func (ii *Insights) handleMessage(record interface{}) {
	ls, ok := record.(*LogStats)
	if !ok {
		log.Infof("not a LogStats: %v (%T)", record, record)
		return
	}

	var sql string
	var comments []string
	// If there was an error, there's no point in parsing ls.SQL, because we're not going to use it.
	if ls.Error == nil {
		// comments with /**/ markers are present in the normalized ls.SQL, and we need to split them off.
		// comments with -- markers get stripped when newExecute calls getPlan around plan_execute.go:63.
		sql, comments = splitComments(ls.SQL)
		sql, ls.Error = normalizeSQL(sql)
	} else {
		sql = "<error>"
	}

	if ls.Error != nil && ls.StmtType == "" {
		ls.StmtType = "ERROR"
	}

	ii.addToAggregates(ls, sql)

	if !ii.shouldSendToInsights(ls) {
		return
	}

	buf, err := ii.makeQueryMessage(ls, sql, parseCommentTags(comments))
	if err != nil {
		log.Warningf("Could not serialize %s message: %v", queryTopic, err)
	} else {
		var kafkaKey string
		if ls.Error != nil {
			kafkaKey = ii.makeKafkaKey(ls.Error.Error())
		} else {
			kafkaKey = ii.makeKafkaKey(sql)
		}
		ii.reserveAndSend(buf, queryTopic, kafkaKey)
	}
}

func (ii *Insights) makeKafkaKey(sql string) string {
	h := hack.RuntimeStrhash(sql, 0x1122334455667788) & math2.MaxUint32
	return ii.DatabaseBranchPublicID + "/" + strconv.FormatUint(h, 16)
}

func (ii *Insights) reserveAndSend(buf []byte, topic, key string) bool {
	reserve := uint64(len(buf))

	if atomic.LoadUint64(&ii.InFlightCounter)+reserve >= uint64(ii.MaxInFlight) {
		// log or increment a dropped message counter
		ii.LogBufferExceeded++
		return false
	}

	err := ii.Sender(buf, topic, key)
	if err != nil {
		log.Warningf("Error sending to Kafka: %v", err)
		return false
	}

	atomic.AddUint64(&ii.InFlightCounter, reserve)
	return true
}

func (ii *Insights) sendToKafka(buf []byte, topic, key string) error {
	return ii.KafkaWriter.WriteMessages(context.Background(),
		kafka.Message{
			Topic: topic,
			Key:   []byte(key),
			Value: buf,
		})
}

func maxDuration(a time.Duration, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func (ii *Insights) addToAggregates(ls *LogStats, sql string) bool {
	// no locks needed if all callers are on the same thread

	var pa *QueryPatternAggregation
	key := QueryPatternKey{
		Keyspace: ls.Keyspace,
	}

	// When there is an error, `sql` isn't normalized.  To protect private information and
	// avoid high cardinality in ii.Aggregations, combine all error statements into a single
	// bucket.
	if ls.Error == nil {
		key.SQL = sql
	} else {
		key.SQL = "<error>"
	}

	pa, ok := ii.Aggregations[key]
	if !ok {
		if uint(len(ii.Aggregations)) >= ii.MaxPatterns {
			ii.LogPatternsExceeded++
			return false
		}
		// ls.StmtType and ls.Table depend only on the contents of sql, so we don't separately make them
		// part of the key, and we don't track them as separate values in the QueryPatternAggregation values.
		// In other words, we assume they don't change, so we only need to track a single value for each.
		pa = newPatternAggregation(ls.StmtType, []string{ls.Table})
		ii.Aggregations[key] = pa
	}

	pa.QueryCount++
	if ls.Error != nil {
		pa.ErrorCount++
	}
	pa.SumShardQueries += ls.ShardQueries
	pa.MaxShardQueries = math.MaxUInt64(pa.MaxShardQueries, ls.ShardQueries)
	pa.SumRowsRead += ls.RowsRead
	pa.MaxRowsRead = math.MaxUInt64(pa.MaxRowsRead, ls.RowsRead)
	pa.SumRowsAffected += ls.RowsAffected
	pa.MaxRowsAffected = math.MaxUInt64(pa.MaxRowsAffected, ls.RowsAffected)
	pa.SumRowsReturned += ls.RowsReturned
	pa.MaxRowsReturned = math.MaxUInt64(pa.MaxRowsReturned, ls.RowsReturned)
	pa.SumTotalDuration += ls.TotalTime()
	pa.MaxTotalDuration = maxDuration(pa.MaxTotalDuration, ls.TotalTime())
	pa.SumPlanDuration += ls.PlanTime
	pa.MaxPlanDuration = maxDuration(pa.MaxPlanDuration, ls.PlanTime)
	pa.SumExecuteDuration += ls.ExecuteTime
	pa.MaxExecuteDuration = maxDuration(pa.MaxExecuteDuration, ls.ExecuteTime)
	pa.SumCommitDuration += ls.CommitTime
	pa.MaxCommitDuration = maxDuration(pa.MaxCommitDuration, ls.CommitTime)

	return true
}

func (ii *Insights) sendAggregates() {
	// no locks needed if all callers are on the same thread

	if len(ii.Aggregations) > 0 {
		log.Infof("Sending aggregates for %v query patterns", len(ii.Aggregations))
	}
	if ii.LogPatternsExceeded > 0 {
		log.Infof("Too many patterns: reached limit of %v.  %v statements not aggregated.", ii.MaxPatterns, ii.LogPatternsExceeded)
		ii.LogPatternsExceeded = 0
	}
	if ii.LogBufferExceeded > 0 {
		log.Infof("Dropped %v Kafka message(s): InFlightCounter=%v, MaxInFlight=%v", ii.LogBufferExceeded, ii.InFlightCounter, ii.MaxInFlight)
		ii.LogBufferExceeded = 0
	}

	for k, pa := range ii.Aggregations {
		buf, err := ii.makeQueryPatternMessage(k.SQL, k.Keyspace, pa)
		if err != nil {
			log.Warningf("Could not serialize %s message: %v", queryStatsBundleTopic, err)
		} else {
			ii.reserveAndSend(buf, queryStatsBundleTopic, ii.makeKafkaKey(k.SQL))
		}
	}

	// remove all accumulated counters
	ii.startInterval()
}

func hostnameOrEmpty() string {
	hostname, err := os.Hostname()
	if err == nil {
		return ""
	}
	return hostname
}

func (ii *Insights) makeQueryMessage(ls *LogStats, sql string, tags []*pbvtgate.Query_Tag) ([]byte, error) {
	addr, user := ls.RemoteAddrUsername()
	var port *wrapperspb.UInt32Value
	if strings.Contains(addr, ":") {
		tok := strings.SplitN(addr, ":", 2)
		p, err := strconv.ParseUint(tok[1], 10, 32)
		if err == nil {
			addr = tok[0]
			port = wrapperspb.UInt32(uint32(p))
		}
	}

	var tables []string
	if ls.Table != "" {
		tables = append(tables, ls.Table)
	}

	obj := pbvtgate.Query{
		StartTime:              timestamppb.New(ls.StartTime),
		DatabaseBranchPublicId: ii.DatabaseBranchPublicID,
		Username:               user,
		RemoteAddress:          stringOrNil(addr),
		RemotePort:             port,
		VtgateName:             hostnameOrEmpty(),
		NormalizedSql:          stringOrNil(sql),
		StatementType:          stringOrNil(ls.StmtType),
		Tables:                 tables, // FIXME: ls.Table captures only one table, and only for simple queries
		Keyspace:               stringOrNil(ls.Keyspace),
		TabletType:             stringOrNil(ls.TabletType),
		ShardQueries:           uint32(ls.ShardQueries),
		RowsRead:               ls.RowsRead,
		RowsAffected:           ls.RowsAffected,
		RowsReturned:           ls.RowsReturned,
		TotalDuration:          durationOrNil(ls.TotalTime()),
		PlanDuration:           durationOrNil(ls.PlanTime),
		ExecuteDuration:        durationOrNil(ls.ExecuteTime),
		CommitDuration:         durationOrNil(ls.CommitTime),
		Error:                  stringOrNil(ls.ErrorStr()),
		CommentTags:            tags,
	}

	var out []byte
	var err error
	if ii.KafkaText {
		out, err = prototext.Marshal(&obj)
	} else {
		out, err = obj.MarshalVT()
	}
	if err != nil {
		return nil, err
	}
	return ii.makeEnvelope(out, queryTopic)
}

func (ii *Insights) makeQueryPatternMessage(sql, keyspace string, pa *QueryPatternAggregation) ([]byte, error) {
	obj := pbvtgate.QueryStatsBundle{
		PeriodStart:            timestamppb.New(ii.PeriodStart),
		DatabaseBranchPublicId: ii.DatabaseBranchPublicID,
		VtgateName:             hostnameOrEmpty(),
		NormalizedSql:          stringOrNil(sql),
		StatementType:          pa.StatementType,
		Tables:                 pa.Tables,
		Keyspace:               stringOrNil(keyspace),
		QueryCount:             pa.QueryCount,
		ErrorCount:             pa.ErrorCount,
		SumShardQueries:        pa.SumShardQueries,
		MaxShardQueries:        pa.MaxShardQueries,
		SumRowsRead:            pa.SumRowsRead,
		MaxRowsRead:            pa.MaxRowsRead,
		SumRowsAffected:        pa.SumRowsAffected,
		MaxRowsAffected:        pa.MaxRowsAffected,
		SumRowsReturned:        pa.SumRowsReturned,
		MaxRowsReturned:        pa.MaxRowsReturned,
		SumTotalDuration:       durationOrNil(pa.SumTotalDuration),
		MaxTotalDuration:       durationOrNil(pa.MaxTotalDuration),
		SumPlanDuration:        durationOrNil(pa.SumPlanDuration),
		MaxPlanDuration:        durationOrNil(pa.MaxPlanDuration),
		SumExecuteDuration:     durationOrNil(pa.SumExecuteDuration),
		MaxExecuteDuration:     durationOrNil(pa.MaxExecuteDuration),
		SumCommitDuration:      durationOrNil(pa.SumCommitDuration),
		MaxCommitDuration:      durationOrNil(pa.MaxCommitDuration),
	}

	var out []byte
	var err error
	if ii.KafkaText {
		out, err = prototext.Marshal(&obj)
	} else {
		out, err = obj.MarshalVT()
	}
	if err != nil {
		return nil, err
	}
	return ii.makeEnvelope(out, queryStatsBundleTopic)
}

func (ii *Insights) makeEnvelope(contents []byte, topic string) ([]byte, error) {
	envelope := pbenvelope.Envelope{
		TypeUrl:   queryURLBase + "/" + topic,
		Event:     contents,
		Id:        uuid.NewString(),
		Timestamp: timestamppb.Now(),
	}

	if ii.KafkaText {
		return prototext.Marshal(&envelope)
	}
	return envelope.MarshalVT()
}

func normalizeSQL(sql string) (string, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		// should never happen since the SQL was already processed
		return "<error>", err
	}

	buf := sqlparser.NewTrackedBuffer(func(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
		switch node := node.(type) {
		case *sqlparser.ComparisonExpr:
			if node.Operator == sqlparser.InOp {
				buf.Myprintf("%l in (<elements>)", node.Left)
			} else {
				node.Format(buf)
			}
		case sqlparser.Values:
			buf.WriteString("values <values>")
		case *sqlparser.Savepoint:
			buf.WriteString("savepoint <id>")
		case *sqlparser.Release:
			buf.WriteString("release savepoint <id>")
		default:
			node.Format(buf)
		}
	})
	return buf.WriteNode(stmt).String(), nil
}

func stringOrNil(s string) *wrapperspb.StringValue {
	if s == "" {
		return nil
	}
	return wrapperspb.String(s)
}

func durationOrNil(d time.Duration) *durationpb.Duration {
	if d == 0 {
		return nil
	}
	return durationpb.New(d)
}

func (ii *Insights) MockTimer() {
	// Send a nil to the LogChan to force a flush.  Only for use in unit tests.
	ii.LogChan <- nil
}
