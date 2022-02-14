package servenv

import (
	"flag"
	"fmt"
	"os"

	"vitess.io/vitess/go/vt/log"
)

var pidFile = flag.String("pid_file", "", "If set, the process will write its pid to the named file, and delete it on graceful shutdown.")

func init() {
	pidFileCreated := false

	// Create pid file after flags are parsed.
	OnInit(func() {
		if *pidFile == "" {
			return
		}

		file, err := os.OpenFile(*pidFile, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
		if err != nil {
			log.Errorf("Unable to create pid file '%s': %v", *pidFile, err)
			return
		}
		pidFileCreated = true
		fmt.Fprintln(file, os.Getpid())
		file.Close()
	})

	// Remove pid file on graceful shutdown.
	OnClose(func() {
		if *pidFile == "" {
			return
		}
		if !pidFileCreated {
			return
		}

		if err := os.Remove(*pidFile); err != nil {
			log.Errorf("Unable to remove pid file '%s': %v", *pidFile, err)
		}
	})
}
