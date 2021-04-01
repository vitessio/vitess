package client

import (
	"database/sql"
	"fmt"
	"strings"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
)

/*
 * This package is meant to provide a client that includes prometheus metrics
 * for common database issues.
 */

var (
	defaultBuckets    = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}
	countErrorLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "are_you_alive_count_error_latency_seconds",
		Help:    "Latency to recieve a count error",
		Buckets: defaultBuckets,
	},
		[]string{"database_name", "tablet_type"},
	)
	readErrorLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "are_you_alive_read_error_latency_seconds",
		Help:    "Latency to recieve a read error",
		Buckets: defaultBuckets,
	},
		[]string{"database_name", "tablet_type"},
	)
	deleteErrorLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "are_you_alive_delete_error_latency_seconds",
		Help:    "Latency to recieve a delete error",
		Buckets: defaultBuckets,
	},
		[]string{"database_name"},
	)
	writeErrorLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "are_you_alive_write_error_latency_seconds",
		Help:    "Latency to recieve a write error",
		Buckets: defaultBuckets,
	},
		[]string{"database_name"},
	)
	connectErrorLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "are_you_alive_connect_error_latency_seconds",
		Help:    "Latency to recieve a connect error",
		Buckets: defaultBuckets,
	},
		[]string{"database_name", "tablet_type"},
	)
	countLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "are_you_alive_count_latency_seconds",
		Help:    "Time it takes to count to the database",
		Buckets: defaultBuckets,
	},
		[]string{"database_name", "tablet_type"},
	)
	readLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "are_you_alive_read_latency_seconds",
		Help:    "Time it takes to read to the database",
		Buckets: defaultBuckets,
	},
		[]string{"database_name", "tablet_type"},
	)
	deleteLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "are_you_alive_delete_latency_seconds",
		Help:    "Time it takes to delete to the database",
		Buckets: defaultBuckets,
	},
		[]string{"database_name"},
	)
	writeLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "are_you_alive_write_latency_seconds",
		Help:    "Time it takes to write to the database",
		Buckets: defaultBuckets,
	},
		[]string{"database_name"},
	)
	connectLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "are_you_alive_connect_latency_seconds",
		Help:    "Time it takes to connect to the database",
		Buckets: defaultBuckets,
	},
		[]string{"database_name", "tablet_type"},
	)
)

// ParseDBName extracts the database name from a mysql connection string.
func ParseDBName(connectionString string) string {
	mysqlConfig, err := mysql.ParseDSN(connectionString)
	if err != nil {
		logrus.WithError(err).Fatal("Error parsing DSN!")
	}
	return mysqlConfig.DBName
}

// ParseTabletType extracts the tablet type from a vitess specific mysql
// connection string.
//
// See https://vitess.io/docs/faq/queries/ for where these come from.
func ParseTabletType(connectionString string) string {
	databaseName := ParseDBName(connectionString)
	if strings.HasSuffix(databaseName, "@master") {
		return "master"
	} else if strings.HasSuffix(databaseName, "@replica") {
		return "replica"
	} else if strings.HasSuffix(databaseName, "@rdonly") {
		return "rdonly"
	} else {
		return "default"
	}
}

func openDatabase(connectionString string) (*sql.DB, error) {
	databaseName := ParseDBName(connectionString)
	tabletType := ParseTabletType(connectionString)
	// NOTE: This is probably not measuring open connections.  I think they
	// connections are created/fetched from the pool when an operation is
	// actually performed.  We could force this with a ping probably, but for
	// now this is here just as a sanity check that this is actually all
	// happening locally.  We should just see everything complete within
	// milliseconds.
	labels := prometheus.Labels{
		"database_name": databaseName,
		"tablet_type":   tabletType}
	connectTimer := prometheus.NewTimer(connectLatency.With(labels))
	connectErrorTimer := prometheus.NewTimer(connectErrorLatency.With(labels))
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		logrus.WithError(err).Error("Error connecting to database")
		connectErrorTimer.ObserveDuration()
		return nil, err
	}
	connectTimer.ObserveDuration()
	return db, nil
}

// InitializeDatabase will connect to the given connectionString, drop the
// given tableName, and recreate it with the schema that the rest of the client
// expects.  This is not something any normal client would do but is convenient
// here because we are just using this client for monitoring.
func InitializeDatabase(connectionString string, tableName string) error {

	// 0. Create logger
	log := logrus.WithField("connection_string", connectionString)

	// 1. Open client to database
	db, err := openDatabase(connectionString)
	if err != nil {
		log.WithError(err).Error("Error opening database")
		return err
	}
	defer db.Close()

	// 2. Delete test table, but continue if it's not there
	if _, err := db.Exec(fmt.Sprintf("DROP TABLE %s", tableName)); err != nil {
		log.WithError(err).Warn("Error deleting database")
	}

	// 3. Create table
	createSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s(page INT, message VARCHAR(255) NOT NULL, PRIMARY KEY (page))", tableName)
	if _, err := db.Exec(createSQL); err != nil {
		log.WithError(err).Error("Error creating database")
		return err
	}
	return nil
}

// WipeTestTable connects to the database given by connectionString and deletes
// everything in the table given by tableName because this client expects the
// table to be empty.  No client would normally do this, but it's convenient for
// testing.
func WipeTestTable(connectionString string, tableName string) error {

	// 0. Create logger
	log := logrus.WithField("connection_string", connectionString)

	// 1. Open client to database
	db, err := openDatabase(connectionString)
	if err != nil {
		log.WithError(err).Error("Error opening database")
		return err
	}
	defer db.Close()

	// 2. Clear database
	if _, err := db.Exec(fmt.Sprintf("DELETE FROM %s", tableName)); err != nil {
		log.WithError(err).Warn("Error clearing table")
	}
	return nil
}

// Write will write the record given by page to the test table in the database
// referenced by connectionString.
func Write(connectionString string, page int) error {

	// 0. Create logger
	log := logrus.WithField("connection_string", connectionString)

	// 1. Open client to database
	databaseName := ParseDBName(connectionString)
	db, err := openDatabase(connectionString)
	if err != nil {
		log.WithError(err).Error("Error opening database")
		return err
	}
	defer db.Close()

	// 2. Write record
	labels := prometheus.Labels{
		"database_name": databaseName}
	writeTimer := prometheus.NewTimer(writeLatency.With(labels))
	writeErrorTimer := prometheus.NewTimer(writeErrorLatency.With(labels))
	if _, err := db.Exec("INSERT INTO are_you_alive_messages (page, message) VALUES (?, ?)", page, "foo"); err != nil {
		log.WithError(err).Error("Error inserting into database")
		writeErrorTimer.ObserveDuration()
		return err
	}
	writeTimer.ObserveDuration()
	return nil
}

// Read will read the record given by page from the test table in the database
// referenced by connectionString.
func Read(connectionString string, page int) (int, string, error) {

	// 0. Create logger
	log := logrus.WithField("connection_string", connectionString)

	// 1. Open client to database
	databaseName := ParseDBName(connectionString)
	db, err := openDatabase(connectionString)
	if err != nil {
		log.WithError(err).Error("Error opening database")
		return 0, "", err
	}
	defer db.Close()

	// 2. Read record
	tabletType := ParseTabletType(connectionString)
	labels := prometheus.Labels{
		"database_name": databaseName,
		"tablet_type":   tabletType}
	readTimer := prometheus.NewTimer(readLatency.With(labels))
	readErrorTimer := prometheus.NewTimer(readErrorLatency.With(labels))
	row := db.QueryRow("SELECT * FROM are_you_alive_messages WHERE page=?", page)
	var readID int
	var readMsg string
	if err := row.Scan(&readID, &readMsg); err != nil {
		if err == sql.ErrNoRows {
			// If our error is just that we didn't find anything, don't treat
			// this as an error or a success.  Just return and let the caller
			// deal with it so we don't mess up our metrics.
			return 0, "", err
		}
		log.WithError(err).Error("Error connecting to database")
		readErrorTimer.ObserveDuration()
		return 0, "", err
	}
	logrus.WithFields(logrus.Fields{
		"readId":  readID,
		"readMsg": readMsg,
	}).Debug("Successfully read row")
	readTimer.ObserveDuration()
	return readID, readMsg, nil
}

// Count will count all the documents in the test table in the database
// referenced by connectionString.
func Count(connectionString string) (int, error) {

	// 0. Create logger
	log := logrus.WithField("connection_string", connectionString)

	// 1. Open client to database
	databaseName := ParseDBName(connectionString)
	db, err := openDatabase(connectionString)
	if err != nil {
		log.WithError(err).Error("Error opening database")
		return 0, err
	}
	defer db.Close()

	// 2. Run Count
	tabletType := ParseTabletType(connectionString)
	labels := prometheus.Labels{
		"database_name": databaseName,
		"tablet_type":   tabletType}
	countTimer := prometheus.NewTimer(countLatency.With(labels))
	countErrorTimer := prometheus.NewTimer(countErrorLatency.With(labels))
	row := db.QueryRow("SELECT COUNT(*) FROM are_you_alive_messages")
	var count int
	if err := row.Scan(&count); err != nil {
		log.WithError(err).Error("Error running count")
		countErrorTimer.ObserveDuration()
		return 0, err
	}
	logrus.WithFields(logrus.Fields{
		"count": count,
	}).Debug("Successfully ran count")
	countTimer.ObserveDuration()
	return count, nil
}

// Delete will delete the record given by page from the test table in the
// database referenced by connectionString.
func Delete(connectionString string, page int) error {

	// 0. Create logger
	log := logrus.WithFields(logrus.Fields{
		"connection_string": connectionString,
		"page":              page,
	})

	// 1. Open client to database
	databaseName := ParseDBName(connectionString)
	labels := prometheus.Labels{
		"database_name": databaseName}
	deleteTimer := prometheus.NewTimer(deleteLatency.With(labels))
	deleteErrorTimer := prometheus.NewTimer(deleteErrorLatency.With(labels))
	db, err := openDatabase(connectionString)
	if err != nil {
		log.WithError(err).Error("Error opening database")
		deleteErrorTimer.ObserveDuration()
		return err
	}
	defer db.Close()

	// 2. Delete record
	if _, err := db.Exec("DELETE FROM are_you_alive_messages WHERE page=?", page); err != nil {
		log.WithError(err).Error("Error deleting record")
		deleteErrorTimer.ObserveDuration()
		return err
	}
	deleteTimer.ObserveDuration()
	return nil
}
