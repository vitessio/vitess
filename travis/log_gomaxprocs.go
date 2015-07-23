// The sole purpose of this file is to print the configured value of GOMAXPROCS.
// This way we can verify that the Travis CI worker is configured correctly by default.
package main

import (
	"fmt"
	"runtime"
)

func main() {
	fmt.Println("GOMAXPROCS is:", runtime.GOMAXPROCS(0))
}
