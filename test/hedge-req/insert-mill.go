package main

import (
	"fmt"
	"vitess.io/vitess/go/vt/vitessdriver"
)

func main() {
	// Connect to vtgate.
	db, err := vitessdriver.Open("localhost:15991", "commerce@primary")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	query := "INSERT INTO corder VALUES "

	n := 10000
	for i := 0; i < n; i++ {
		a := ",\n"
		if i == n-1 {
			a = ""
		}
		query = query + fmt.Sprintf("(%d, 1, 'A', 1000)%s", i, a)
	}

	fmt.Println(query)

	_, err = db.Exec(query)

	// if there is an error inserting, handle it
	if err != nil {
		panic(err.Error())
	}

}
