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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/streamlog"
)

var (
	logger *streamlog.StreamLogger
)

type setupOptions struct {
	bufSize, patternLimit, rowsReadThreshold, responseTimeThreshold uint
}

func setup(t *testing.T, brokers, publicID, username, password string, options setupOptions) (*Insights, error) {
	logger = streamlog.New("tests", 10)
	dfl := func(x, y uint) uint {
		if x != 0 {
			return x
		}
		return y
	}
	insights, err := initInsightsInner(logger, brokers, publicID, username, password,
		dfl(options.bufSize, 5*1024*1024),
		dfl(options.patternLimit, 10000),
		dfl(options.rowsReadThreshold, 1000),
		dfl(options.responseTimeThreshold, 1000),
		15*time.Second)
	if insights != nil {
		t.Cleanup(func() { insights.Drain() })
	}
	return insights, err
}

func TestInsightsNeedsDatabaseBranchID(t *testing.T) {
	_, err := setup(t, "localhost:1234", "", "", "", setupOptions{})
	assert.Error(t, err, "public_id is required")
}

func TestInsightsDisabled(t *testing.T) {
	_, err := setup(t, "", "", "", "", setupOptions{})
	assert.NoError(t, err)
}

func TestInsightsEnabled(t *testing.T) {
	_, err := setup(t, "localhost:1234", "mumblefoo", "", "", setupOptions{})
	assert.NoError(t, err)
}

func TestInsightsMissingUsername(t *testing.T) {
	_, err := setup(t, "localhost:1234", "mumblefoo", "", "password", setupOptions{})
	assert.Error(t, err, "without a username")
}

func TestInsightsMissingPassword(t *testing.T) {
	_, err := setup(t, "localhost:1234", "mumblefoo", "username", "", setupOptions{})
	assert.Error(t, err, "without a password")
}

func TestInsightsConnectionRefused(t *testing.T) {
	// send to a real Kafka endpoint, will fail
	insights, err := setup(t, "localhost:1", "mumblefoo", "", "", setupOptions{})
	require.NoError(t, err)
	logger.Send(lsSlowQuery)
	require.True(t, insights.Drain(), "did not drain")
}

func TestInsightsSlowQuery(t *testing.T) {
	insights, err := setup(t, "localhost:1234", "mumblefoo", "", "", setupOptions{})
	require.NoError(t, err)
	messages := 0
	insights.Sender = func(buf []byte, topic, key string) error {
		messages++
		assert.Contains(t, string(buf), "select sleep(5)")
		assert.Equal(t, queryTopic, topic)
		return nil
	}
	logger.Send(lsSlowQuery)
	require.True(t, insights.Drain(), "did not drain")
	assert.Equal(t, 1, messages)
}

func TestInsightsSummaries(t *testing.T) {
	insightsTestHelper(t, true, setupOptions{},
		[]insightsQuery{
			{sql: "select sleep(5)", responseTime: 5 * time.Second},
			{sql: "select * from foo", responseTime: 10 * time.Millisecond},
			{sql: "select * from foo", responseTime: 10 * time.Millisecond},
			{sql: "select * from foo", responseTime: 10 * time.Millisecond},
			{sql: "select * from foo", responseTime: 10 * time.Millisecond},
		},
		[]insightsKafkaExpectation{
			{pattern: "select sleep(5)", topic: queryTopic, count: 1},
			{pattern: "select sleep(5)", topic: queryStatsBundleTopic, count: 1},
			{pattern: "select * from foo", topic: queryStatsBundleTopic, count: 1},
		})
}

func TestInsightsTooManyPatterns(t *testing.T) {
	insightsTestHelper(t, true,
		setupOptions{patternLimit: 3},
		[]insightsQuery{
			{sql: "select * from foo1", responseTime: 5 * time.Second},
			{sql: "select * from foo2", responseTime: 5 * time.Second},
			{sql: "select * from foo3", responseTime: 5 * time.Second},
			{sql: "select * from foo4", responseTime: 5 * time.Second},
			{sql: "select * from foo5", responseTime: 5 * time.Second},
		},
		[]insightsKafkaExpectation{
			{pattern: "select * from foo1", topic: queryTopic, count: 1},
			{pattern: "select * from foo2", topic: queryTopic, count: 1},
			{pattern: "select * from foo3", topic: queryTopic, count: 1},
			{pattern: "select * from foo4", topic: queryTopic, count: 1},
			{pattern: "select * from foo5", topic: queryTopic, count: 1},
			{pattern: "select * from foo1", topic: queryStatsBundleTopic, count: 1},
			{pattern: "select * from foo2", topic: queryStatsBundleTopic, count: 1},
			{pattern: "select * from foo3", topic: queryStatsBundleTopic, count: 1},
		})
}

func TestInsightsResponseTimeThreshold(t *testing.T) {
	insightsTestHelper(t, false,
		setupOptions{responseTimeThreshold: 500},
		[]insightsQuery{
			{sql: "select * from foo1", responseTime: 400 * time.Millisecond},
			{sql: "select * from foo2", responseTime: 600 * time.Millisecond},
		},
		[]insightsKafkaExpectation{
			{pattern: "select * from foo2", topic: queryTopic, count: 1},
		})
}

func TestInsightsRowsReadThreshold(t *testing.T) {
	insightsTestHelper(t, false,
		setupOptions{rowsReadThreshold: 42},
		[]insightsQuery{
			{sql: "select * from foo1", responseTime: 5 * time.Millisecond, rowsRead: 88},
			{sql: "select * from foo2", responseTime: 5 * time.Millisecond, rowsRead: 15},
		},
		[]insightsKafkaExpectation{
			{pattern: "select * from foo1", topic: queryTopic, count: 1},
		})
}

func TestInsightsKafkaBufferSize(t *testing.T) {
	insightsTestHelper(t, false,
		setupOptions{bufSize: 5},
		[]insightsQuery{
			{sql: "select * from foo1", responseTime: 5 * time.Second},
		},
		nil)
}

type insightsQuery struct {
	sql          string
	responseTime time.Duration
	rowsRead     int
}

type insightsKafkaExpectation struct {
	pattern, topic string
	count, found   int
}

func insightsTestHelper(t *testing.T, sleep bool, options setupOptions, queries []insightsQuery, expect []insightsKafkaExpectation) {
	t.Helper()
	insights, err := setup(t, "localhost:1234", "mumblefoo", "", "", options)
	require.NoError(t, err)
	insights.Sender = func(buf []byte, topic, key string) error {
		assert.Contains(t, string(buf), "mumblefoo", "database branch public ID not present in message body")
		assert.True(t, strings.HasPrefix(key, "mumblefoo/"), "key has unexpected form %q", key)
		assert.Contains(t, string(buf), queryURLBase+"/"+topic, "expected key not present in message body")
		var found bool
		for i, ex := range expect {
			if strings.Contains(string(buf), ex.pattern) && topic == ex.topic {
				expect[i].found++
				found = true
				break
			}
		}
		assert.True(t, found, "no pattern expects topic=%q buf=%q", topic, string(buf))
		return nil
	}
	for _, q := range queries {
		logger.Send(&LogStats{
			SQL:       q.sql,
			StartTime: time.Now().Add(-q.responseTime),
			EndTime:   time.Now(),
			RowsRead:  uint64(q.rowsRead),
			Ctx:       context.Background(),
		})
	}
	if sleep {
		insights.MockTimer()
	}
	require.True(t, insights.Drain(), "did not drain")
	for _, ex := range expect {
		assert.Equal(t, ex.count, ex.found, "count for topic=%q pattern=%q was wrong", ex.topic, ex.pattern)
	}
}

var (
	lsSlowQuery = &LogStats{
		SQL:       "select sleep(5)",
		StartTime: time.Now().Add(-5 * time.Second),
		EndTime:   time.Now(),
		Ctx:       context.Background(),
	}
)
