package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"

	"github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"go.uber.org/ratelimit"
	"gopkg.in/yaml.v2"

	"vitess.io/vitess/examples/are-you-alive/pkg/client"
)

/*
* To measure data loss, we need predictable writes.  We do this with "minPage"
* and "maxPage".  Once the difference between them is our desired dataset size,
* we can start deleting old records, but we expect to find one record for every
* "page" number between "minPage" and "maxPage".
*
* We don't measure "update loss" with this client right now.
 */

var (
	maxPage        = 0
	minPage        = 0
	dataLossEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "are_you_alive_data_loss_events",
		Help: "Data loss events",
	},
		[]string{"database_name"},
	)
)

func writeNextRecord(connectionString string) error {

	// 1. Call monitored client
	err := client.Write(connectionString, maxPage)
	if err != nil {
		// Check to see if this is a duplicate key error.  We've seen this
		// sometimes happen, and when it does this client app gets stuck in an
		// infinite loop of failure to write a duplicate key.  It's possible
		// that happens because a write is succesful but something goes wrong
		// before the client recieves a response, so the client thinks the write
		// failed and does not increment the count.
		//
		// So when we specifically see a duplicate key error, assume that's what
		// happened, bump the count, and move on.
		//
		// See https://github.com/planetscale/planetscale-operator/issues/1776
		if me, ok := err.(*mysql.MySQLError); ok && me.Number == 1062 {
			logrus.WithError(err).Warnf(
				"Key '%d' already found, incrementing count", maxPage)
			maxPage = maxPage + 1
			return nil
		}

		logrus.WithError(err).Error("Error writing record")
		return err
	}

	// 2. Increment "maxPage"
	maxPage = maxPage + 1
	return nil
}

func readRandomRecord(connectionString string) error {

	// 1. Pick Random Number Between "minPage" and "maxPage"
	if minPage == maxPage {
		logrus.Warn("Nothing has been inserted yet!")
		return nil
	}
	page := (rand.Int() % (maxPage - minPage)) + minPage

	// 2. Read Record
	readID, readMsg, err := client.Read(connectionString, page)
	if err != nil {
		if err == sql.ErrNoRows {
			// This races with deletion, but if our page is greater than minPage
			// we know that it should be in there.  If it's less than minPage
			// assume we are just racing the deletion goroutine and ignore the
			// error.
			if page <= minPage {
				return nil
			}
			// For replicas, there is a chance we are suffering from replication
			// lag, so ignore the missing row if we are a replica.
			// TODO: Should we attempt to roughly figure out replication lag in
			// this client, at least to catch major failures?  We could probably
			// multiply delay by the difference betwen maxCount and the page we
			// are trying to read to figure out how long ago the row we were
			// trying to write was written.
			if client.ParseTabletType(connectionString) == "replica" ||
				client.ParseTabletType(connectionString) == "rdonly" {
				return nil
			}
			logrus.WithError(err).WithFields(logrus.Fields{
				"page":    page,
				"minPage": minPage,
				"maxPage": maxPage,
			}).Error("Query succeeded but record not found, may mean data loss")
			dataLossEvents.With(
				prometheus.Labels{"database_name": client.ParseDBName(connectionString)}).Inc()
			return err
		}
		logrus.WithError(err).Error("Error reading record")
		return err
	}
	// Add zero here just so the metric exists for this database, even if it's
	// zero.
	dataLossEvents.With(
		prometheus.Labels{"database_name": client.ParseDBName(connectionString)}).Add(0)
	logrus.WithFields(logrus.Fields{
		"readID":  readID,
		"readMsg": readMsg,
	}).Debug("Read row!")

	return nil
}

func runCount(connectionString string) error {

	// 1. Run Count
	count, err := client.Count(connectionString)
	if err != nil {
		logrus.WithError(err).Error("Error counting records")
		return err
	}
	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("Counted rows!")

	// 2. Log if COUNT != "minPage" - "maxPage"
	return nil
}

func deleteLastRecordIfNecessary(connectionString string) error {

	// 1. Compare "maxPage" - "minPage" to Desired Dataset Size
	if (maxPage - minPage) < *datasetSize {
		return nil
	}
	logrus.WithFields(logrus.Fields{
		"current": maxPage - minPage,
		"desired": *datasetSize,
	}).Debug("Deleting last record")

	// 2. Delete Record If We Are Above Desired Size
	err := client.Delete(connectionString, minPage)
	if err != nil {
		logrus.WithError(err).Error("Error deleting record")
		return err
	}

	// 3. Increment "minPage"
	minPage = minPage + 1
	return nil
}

var (
	prometheusMetricsAddress = flag.String(
		"prometheus_metrics_address", ":8080", "Address on which to serve prometheus metrics")
	debug                   = flag.Bool("debug", false, "Enable debug logging")
	useVtgate               = flag.Bool("vtgate", false, "Using vtgate (for @master and @replica)")
	initialize              = flag.Bool("initialize", false, "Initialize database (for testing)")
	datasetSize             = flag.Int("dataset_size", 10, "Number of total records in database")
	endpointsConfigFilename = flag.String("endpoints_config", "", "Endpoint and load configuration.")
)

type runner struct {
	connString   string
	fn           func(string) error
	errMessage   string
	opsPerSecond int
}

func (r *runner) run() {
	rl := ratelimit.New(r.opsPerSecond)
	for {
		_ = rl.Take()
		err := r.fn(r.connString)
		if err != nil {
			logrus.WithError(err).Error(r.errMessage)
		}
	}
}

func waitForCtrlC() {
	var endWaiter sync.WaitGroup
	endWaiter.Add(1)
	var signalChannel chan os.Signal
	signalChannel = make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt)
	go func() {
		<-signalChannel
		endWaiter.Done()
	}()
	endWaiter.Wait()
}

func runPrometheus() {
	http.Handle("/metrics", promhttp.Handler())
	logrus.Fatal(http.ListenAndServe(*prometheusMetricsAddress, nil))
}

func loadEndpointsConfig(endpointsConfigFilename string) (endpointsConfig, error) {
	if endpointsConfigFilename == "" {
		return endpointsConfig{}, errors.New("You must pass an endpoints configuration file")
	}

	f, err := os.Open(endpointsConfigFilename)
	if err != nil {
		return endpointsConfig{}, err
	}
	defer f.Close()

	var cfg endpointsConfig
	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&cfg)
	if err != nil {
		return endpointsConfig{}, err
	}
	return cfg, nil
}

type endpointsConfig struct {
	Endpoints []struct {
		ConnectionString       string `yaml:"connectionString"`
		TargetCountsPerSecond  int    `yaml:"targetCountsPerSecond"`
		TargetQueriesPerSecond int    `yaml:"targetQueriesPerSecond"`
		TargetWritesPerSecond  int    `yaml:"targetWritesPerSecond"`
	} `yaml:"endpoints"`
}

func main() {

	// 0. Handle Arguments
	flag.Parse()
	if *debug {
		logrus.SetLevel(logrus.DebugLevel)
	}
	logrus.WithFields(logrus.Fields{
		"endpointsConfigFilename":  *endpointsConfigFilename,
		"prometheusMetricsAddress": *prometheusMetricsAddress,
		"debug":                    *debug,
	}).Debug("Command line arguments")

	var endpoints endpointsConfig
	endpoints, err := loadEndpointsConfig(*endpointsConfigFilename)
	if err != nil {
		logrus.WithError(err).Error("Failed to load endpoints config.")
		os.Exit(1)
	}

	// 0. Set Up Prometheus Metrics
	logrus.Info("Prometheus Go")
	go runPrometheus()

	// 1. Pass "interpolateParams"
	for _, endpoint := range endpoints.Endpoints {
		logrus.WithFields(logrus.Fields{
			"connectionString":       endpoint.ConnectionString,
			"targetCountsPerSecond":  endpoint.TargetCountsPerSecond,
			"targetQueriesPerSecond": endpoint.TargetQueriesPerSecond,
			"targetWritesPerSecond":  endpoint.TargetWritesPerSecond,
		}).Info("Found endpoint configuration")

		// We need to pass interpolateParams when using a vtgate because
		// prepare is not supported.
		//
		// See:
		// - https://github.com/go-sql-driver/mysql/blob/master/README.md#interpolateparams
		// - https://github.com/src-d/go-mysql-server/issues/428
		// - https://github.com/vitessio/vitess/pull/3862
		endpoint.ConnectionString = fmt.Sprintf("%s?interpolateParams=true", endpoint.ConnectionString)
	}

	// 2. Initialize Database
	for _, endpoint := range endpoints.Endpoints {
		logrus.WithFields(logrus.Fields{
			"connectionString":       endpoint.ConnectionString,
			"targetCountsPerSecond":  endpoint.TargetCountsPerSecond,
			"targetQueriesPerSecond": endpoint.TargetQueriesPerSecond,
			"targetWritesPerSecond":  endpoint.TargetWritesPerSecond,
		}).Info("Found endpoint configuration")

		if endpoint.TargetWritesPerSecond > 0 {
			logrus.Info("Initializing database")
			// For local testing, does not initialize vschema
			if *initialize {
				client.InitializeDatabase(endpoint.ConnectionString, "are_you_alive_messages")
			}
			client.WipeTestTable(endpoint.ConnectionString, "are_you_alive_messages")
		}
	}

	// 3. Start Client Goroutines
	logrus.Info("Starting client goroutines")
	for _, endpoint := range endpoints.Endpoints {
		logrus.WithFields(logrus.Fields{
			"connectionString":       endpoint.ConnectionString,
			"targetCountsPerSecond":  endpoint.TargetCountsPerSecond,
			"targetQueriesPerSecond": endpoint.TargetQueriesPerSecond,
			"targetWritesPerSecond":  endpoint.TargetWritesPerSecond,
		}).Info("Found endpoint configuration")

		if endpoint.TargetWritesPerSecond > 0 {
			writer := runner{
				connString:   endpoint.ConnectionString,
				fn:           writeNextRecord,
				errMessage:   "Recieved error writing next record",
				opsPerSecond: endpoint.TargetWritesPerSecond,
			}
			go writer.run()
			deleter := runner{
				connString:   endpoint.ConnectionString,
				fn:           deleteLastRecordIfNecessary,
				errMessage:   "Recieved error deleting last record",
				opsPerSecond: 100, // This is based on target "dataset_size", and will not make a query if not needed.  TODO: Actually tune this in a reasonable way after redesigning the schema?
			}
			go deleter.run()
		}
		if endpoint.TargetQueriesPerSecond > 0 {
			reader := runner{
				connString:   endpoint.ConnectionString,
				fn:           readRandomRecord,
				errMessage:   "Recieved error reading record",
				opsPerSecond: endpoint.TargetQueriesPerSecond,
			}
			go reader.run()
		}
		if endpoint.TargetCountsPerSecond > 0 {
			counter := runner{
				connString:   endpoint.ConnectionString,
				fn:           runCount,
				errMessage:   "Recieved error running count",
				opsPerSecond: endpoint.TargetCountsPerSecond,
			}
			go counter.run()
		}
	}

	logrus.Info("Press Ctrl+C to end\n")
	waitForCtrlC()
	logrus.Info("\n")
}
