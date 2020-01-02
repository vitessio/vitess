package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/planetscale/are-you-alive/pkg/client"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"
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

func writeNextRecord(environmentName string, connectionString string) error {

	// 1. Call monitored client
	err := client.Write(environmentName, connectionString, maxPage)
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

func readRandomRecord(environmentName string, connectionString string) error {

	// 1. Pick Random Number Between "minPage" and "maxPage"
	if minPage == maxPage {
		logrus.Warn("Nothing has been inserted yet!")
		return nil
	}
	page := (rand.Int() % (maxPage - minPage)) + minPage

	// 2. Read Record
	readID, readMsg, err := client.Read(environmentName, connectionString, page)
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

func runCount(environmentName string, connectionString string) error {

	// 1. Run Count
	count, err := client.Count(environmentName, connectionString)
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

func deleteLastRecordIfNecessary(environmentName string, connectionString string) error {

	// 1. Compare "maxPage" - "minPage" to Desired Dataset Size
	if (maxPage - minPage) < *datasetSize {
		return nil
	}
	logrus.WithFields(logrus.Fields{
		"current": maxPage - minPage,
		"desired": *datasetSize,
	}).Debug("Deleting last record")

	// 2. Delete Record If We Are Above Desired Size
	err := client.Delete(environmentName, connectionString, minPage)
	if err != nil {
		logrus.WithError(err).Error("Error deleting record")
		return err
	}

	// 3. Increment "minPage"
	minPage = minPage + 1
	return nil
}

var (
	mysqlConnectionString = flag.String(
		"mysql_connection_string", "", "Connection string for db to test")
	prometheusMetricsAddress = flag.String(
		"prometheus_metrics_address", ":8080", "Address on which to serve prometheus metrics")
	debug            = flag.Bool("debug", false, "Enable debug logging")
	useVtgate        = flag.Bool("vtgate", false, "Using vtgate (for @master and @replica)")
	readFromReplica  = flag.Bool("replica", false, "Read from replica")
	readFromReadOnly = flag.Bool("rdonly", false, "Read from rdonly")
	initialize       = flag.Bool("initialize", false, "Initialize database (for testing)")
	sleepTime        = flag.Int("delay", 1*1000*1000*1000, "Delay in nanoseconds between ops")
	datasetSize      = flag.Int("dataset_size", 10, "Number of total records in database")
	environmentName  = flag.String("environment_name", "prod",
		"Environment the database is deployed in that this client is pointing at")
)

type runner struct {
	connString string
	envName    string
	fn         func(string, string) error
	errMessage string
	sleepTime  time.Duration
}

func (r *runner) run() {
	for {
		time.Sleep(r.sleepTime)
		err := r.fn(r.envName, r.connString)
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

func main() {

	// 0. Handle Arguments
	flag.Parse()
	if *debug {
		logrus.SetLevel(logrus.DebugLevel)
	}
	logrus.WithFields(logrus.Fields{
		"mysqlConnectionString":    *mysqlConnectionString,
		"prometheusMetricsAddress": *prometheusMetricsAddress,
		"debug":                    *debug,
	}).Debug("Command line arguments")

	connectionString := ""
	if *mysqlConnectionString != "" {
		connectionString = *mysqlConnectionString
	} else if os.Getenv("MYSQL_CONN_STRING") != "" {
		connectionString = os.Getenv("MYSQL_CONN_STRING")
	}
	masterConnectionString := connectionString
	replicaConnectionString := connectionString
	rdonlyConnectionString := connectionString
	// When using vtgate, we want to append @master and @replica to the DSN, but
	// this will fail against normal mysql which we're using for testing.  See:
	// https://vitess.io/docs/user-guides/faq/#how-do-i-choose-between-master-vs-replica-for-queries
	if *useVtgate {
		// We need to pass interpolateParams when using a vtgate because
		// prepare is not supported.
		//
		// See:
		// - https://github.com/go-sql-driver/mysql/blob/master/README.md#interpolateparams
		// - https://github.com/src-d/go-mysql-server/issues/428
		// - https://github.com/vitessio/vitess/pull/3862
		masterConnectionString = fmt.Sprintf("%s@master?interpolateParams=true", connectionString)
		replicaConnectionString = fmt.Sprintf("%s@replica?interpolateParams=true", connectionString)
		rdonlyConnectionString = fmt.Sprintf("%s@rdonly?interpolateParams=true", connectionString)
	}
	fmt.Println("masterConnectionString:", masterConnectionString)
	fmt.Println("replicaConnectionString:", replicaConnectionString)
	fmt.Println("rdonlyConnectionString:", rdonlyConnectionString)

	// 1. Set Up Prometheus Metrics
	logrus.Info("Prometheus Go")
	go runPrometheus()

	// 2. Initialize Database
	logrus.Info("Initializing database")
	// For local testing, does not initialize vschema
	if *initialize {
		client.InitializeDatabase(*environmentName, masterConnectionString, "are_you_alive_messages")
	}
	client.WipeTestTable(*environmentName, masterConnectionString, "are_you_alive_messages")

	// 3. Start goroutines to do various things
	logrus.Info("Starting client goroutines")
	deleter := runner{
		connString: masterConnectionString,
		envName:    *environmentName,
		fn:         deleteLastRecordIfNecessary,
		errMessage: "Recieved error deleting last record",
		sleepTime:  time.Duration(*sleepTime),
	}
	go deleter.run()
	writer := runner{
		connString: masterConnectionString,
		envName:    *environmentName,
		fn:         writeNextRecord,
		errMessage: "Recieved error writing next record",
		sleepTime:  time.Duration(*sleepTime),
	}
	go writer.run()
	reader := runner{
		connString: masterConnectionString,
		envName:    *environmentName,
		fn:         readRandomRecord,
		errMessage: "Recieved error reading record",
		sleepTime:  time.Duration(*sleepTime),
	}
	go reader.run()
	counter := runner{
		connString: masterConnectionString,
		envName:    *environmentName,
		fn:         runCount,
		errMessage: "Recieved error running count",
		sleepTime:  time.Duration(*sleepTime),
	}
	go counter.run()

	// Only bother starting a replica reader/counter if we are using a vtgate
	// and actually are asking to do replica reads
	if *useVtgate && *readFromReplica {
		replicaReader := runner{
			connString: replicaConnectionString,
			envName:    *environmentName,
			fn:         readRandomRecord,
			errMessage: "Recieved error reading record from replica",
			sleepTime:  time.Duration(*sleepTime),
		}
		go replicaReader.run()
		replicaRowCounter := runner{
			connString: replicaConnectionString,
			envName:    *environmentName,
			fn:         runCount,
			errMessage: "Recieved error running count on replica",
			sleepTime:  time.Duration(*sleepTime),
		}
		go replicaRowCounter.run()
	}

	// Only bother starting a rdonly reader/counter if we are using a vtgate and
	// actually are asking to do rdonly reads
	if *useVtgate && *readFromReadOnly {
		replicaReader := runner{
			connString: rdonlyConnectionString,
			envName:    *environmentName,
			fn:         readRandomRecord,
			errMessage: "Recieved error reading record from rdonly",
			sleepTime:  time.Duration(*sleepTime),
		}
		go replicaReader.run()
		replicaRowCounter := runner{
			connString: rdonlyConnectionString,
			envName:    *environmentName,
			fn:         runCount,
			errMessage: "Recieved error running count on rdonly",
			sleepTime:  time.Duration(*sleepTime),
		}
		go replicaRowCounter.run()
	}

	logrus.Info("Press Ctrl+C to end\n")
	waitForCtrlC()
	logrus.Info("\n")
}
