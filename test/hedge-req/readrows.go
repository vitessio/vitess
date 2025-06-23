package main

import (
	"fmt"
	"sync"
	"time"
	"vitess.io/vitess/go/vt/vitessdriver"
)

func makeQuery(times chan time.Duration, errors chan error) {
	// Connect to vtgate.
	db, err := vitessdriver.Open("localhost:15991", "commerce@replica")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	query := "SELECT * FROM corder WHERE order_id > 2000"

	startTime := time.Now()
	_, err = db.Query(query)

	if err != nil {
		errors <- err
		return
	}

	times <- time.Since(startTime)
}

func timedQueries(sleepTime time.Duration, numberOfQueries int, wg *sync.WaitGroup, times chan time.Duration, errors chan error) {
	for i := 0; i < numberOfQueries; i++ {
		wg.Add(1)
		go func() {
			makeQuery(times, errors)
			wg.Done()
		}()
		time.Sleep(sleepTime)
	}
}

func main() {
	start := time.Now()

	rate := 100
	timeS := 3
	n := timeS * rate
	durationMS := int(1000 * timeS / rate) // try to choose time_s , rate such that duration_ms is integer

	times := make(chan time.Duration, n)
	errors := make(chan error, n)
	var wg sync.WaitGroup

	timedQueries(time.Duration(durationMS)*time.Millisecond, n, &wg, times, errors)

	wg.Wait()

	//for {
	//	done := false
	//	select {
	//	case val := <-times:
	//		fmt.Println(val)
	//	default:
	//		done = true
	//	}
	//	if done {
	//		break
	//	}
	//}

	errs := 0
	for {
		done := false
		select {
		case err := <-errors:
			fmt.Println(err)
			errs++
		default:
			done = true
		}
		if done {
			break
		}
	}

	fmt.Println(errs)
	fmt.Println(time.Since(start))
}
