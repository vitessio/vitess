package zookeeper

// This file defines methods on Server that deal with starting
// and stopping the ZooKeeper service. They are independent of ZooKeeper
// itself, and may be factored out at a later date.

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"
)

// ErrNotRunning is the error returned when Process cannot
// find the currently running zookeeper process.
var ErrNotRunning = errors.New("process not running")

// Process returns a Process referring to the running server from
// where it's been stored in pid.txt. If the file does not
// exist, or it cannot find the process, it returns the error
// ErrNotRunning.
func (srv *Server) Process() (*os.Process, error) {
	data, err := ioutil.ReadFile(srv.path("pid.txt"))
	if err != nil {
		if os.IsNotExist(err) {
			err = ErrNotRunning
		}
		return nil, err
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		return nil, errors.New("bad process id found in pid.txt")
	}
	return getProcess(pid)
}

// getProcess gets a Process from a pid and checks that the
// process is actually running. If the process
// is not running, then getProcess returns a nil
// Process and the error ErrNotRunning.
func getProcess(pid int) (*os.Process, error) {
	p, err := os.FindProcess(pid)
	if err != nil {
		return nil, err
	}

	// try to check if the process is actually running by sending
	// it signal 0.
	err = p.Signal(syscall.Signal(0))
	if err == nil {
		return p, nil
	}
	if err == syscall.ESRCH {
		return nil, ErrNotRunning
	}
	return nil, errors.New("server running but inaccessible")
}

// Start starts the ZooKeeper server.
// It returns an error if the server is already running.
func (srv *Server) Start() error {
	if err := srv.checkAvailability(); err != nil {
		return err
	}
	p, err := srv.Process()
	if err == nil || err != ErrNotRunning {
		if p != nil {
			p.Release()
		}
		return errors.New("server is already running")
	}

	if _, err := os.Stat(srv.path("pid.txt")); err == nil {
		// Thre pid.txt file still exists although server is not running.
		// Remove it so it can be re-created.
		// This leads to a race: if two processes are both
		// calling Start, one might remove the file the other
		// has just created, leading to a situation where
		// pid.txt describes the wrong server process.
		// We ignore that possibility for now.
		// TODO use syscall.Flock?
		if err := os.Remove(srv.path("pid.txt")); err != nil {
			return fmt.Errorf("cannot remove pid.txt: %v", err)
		}
	}

	// Open the pid file before starting the process so that if we get two
	// programs trying to concurrently start a server on the same directory
	// at the same time, only one should succeed.
	pidf, err := os.OpenFile(srv.path("pid.txt"), os.O_EXCL|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("cannot create pid.txt: %v", err)
	}
	defer pidf.Close()
	args, err := srv.command()
	if err != nil {
		return fmt.Errorf("cannot determine command: %v", err)
	}
	cmd := exec.Command(args[0], args[1:]...)

	logf, err := os.OpenFile(srv.path("log.txt"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("cannot create log file: %v", err)
	}
	defer logf.Close()
	cmd.Stdout = logf
	cmd.Stderr = logf
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("cannot start server: %v", err)
	}
	if _, err := fmt.Fprint(pidf, cmd.Process.Pid); err != nil {
		return fmt.Errorf("cannot write pid file: %v", err)
	}
	return nil
}

// Stop kills the ZooKeeper server. It does nothing if it is not running.
// Note that Stop does not remove any data from the run directory,
// so Start may be called later on the same directory.
func (srv *Server) Stop() error {
	p, err := srv.Process()
	if p == nil {
		if err != nil && err != ErrNotRunning {
			return fmt.Errorf("cannot read process ID of server: %v", err)
		}
		return nil
	}
	defer p.Release()
	if err := p.Kill(); err != nil {
		return fmt.Errorf("cannot kill server process: %v", err)
	}
	// Ignore the error returned from Wait because there's little
	// we can do about it - it either means that the process has just exited
	// anyway or that we can't wait for it for some other reason,
	// for example because it was originally started by some other process.
	if _, err := p.Wait(); err != nil {
		// If we can't wait for the server, it's possible that it was running
		// but not as a child of this process, so the only thing we can do
		// is to poll until it exits. If the process has taken longer than
		// a second to exit, then it's probably not going to.
		for i := 0; i < 5*4; i++ {
			time.Sleep(1e9 / 4)
			if np, err := getProcess(p.Pid); err != nil {
				break
			} else {
				np.Release()
			}
		}
	}

	if err := os.Remove(srv.path("pid.txt")); err != nil {
		return fmt.Errorf("cannot remove server process ID file: %v", err)
	}
	return nil
}

// Destroy stops the ZooKeeper server, and then removes its run
// directory. Warning: this will destroy all data associated with the server.
func (srv *Server) Destroy() error {
	if err := srv.Stop(); err != nil {
		return err
	}
	if err := os.RemoveAll(srv.runDir); err != nil {
		return err
	}
	return nil
}
