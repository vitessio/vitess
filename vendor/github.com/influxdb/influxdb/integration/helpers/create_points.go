package helpers

import (
	"fmt"
	"math/rand"

	influxdb "github.com/influxdb/influxdb/client"
)

func CreatePoints(name string, numOfColumns, numOfPoints int) []*influxdb.Series {
	return CreatePointsFromFunc(name, numOfColumns, numOfPoints, func(int) float64 { return rand.Float64() })
}

func CreateSeries(prefix string, numOfSeries int) []*influxdb.Series {
	s := make([]*influxdb.Series, numOfSeries)
	for i := range s {
		s[i] = &influxdb.Series{
			Name:    fmt.Sprintf("%s%d", prefix, i),
			Columns: []string{"column"},
			Points:  [][]interface{}{{1}},
		}
	}
	return s
}

func CreatePointsFromFunc(name string, numOfColumns, numOfPoints int, f func(int) float64) []*influxdb.Series {
	series := &influxdb.Series{}

	series.Name = name
	for i := 0; i < numOfColumns; i++ {
		series.Columns = append(series.Columns, fmt.Sprintf("column%d", i))
	}

	for i := 0; i < numOfPoints; i++ {
		point := []interface{}{}
		for j := 0; j < numOfColumns; j++ {
			point = append(point, f(i))
		}
		series.Points = append(series.Points, point)
	}

	return []*influxdb.Series{series}
}
