package worker

import "testing"

// TODO I want this test to work using a docker cluster much like our docker compose

type VitessCluster struct{}

func NewVitessClusterWithLookupTables(t *testing.T) VitessCluster {
	return VitessCluster{}
}

// InsertMovies inserts a bunch of movies
func (cluster VitessCluster) InsertMovies(count int) {

}

// DirectSQL runs some raw SQL directly against the specified tablet mysqld
func (cluster VitessCluster) DirectSQL(tablet string, sql string) {

}

// Worker runs the specified worker passing in arguments
func (cluster VitessCluster) Worker(name string, args ...string) {

}

type ResultSet struct{}

func (rs ResultSet) SingleInt() int {
	return 1000
}

// Query runs a query against the vtgate
func (cluster VitessCluster) Query(sql string) ResultSet {
	return ResultSet{}
}

func TestBackfill(t *testing.T) {
	vitessCluster := NewVitessClusterWithLookupTables(t)
	vitessCluster.InsertMovies(1000)
	vitessCluster.DirectSQL("201", "DELETE * FROM movie_name_lookup")
	vitessCluster.Worker("backfill-lookup",
		"-tablet_type", "REPLICA",
		"movies", "movie_name_lookup")
	if vitessCluster.Query("SELECT count(*) FROM movie_name_lookup").SingleInt() != 1000 {
		t.Fail()
	}
}
