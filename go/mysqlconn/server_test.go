package mysqlconn

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	vtenv "github.com/youtube/vitess/go/vt/env"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

var selectRowsResult = &sqltypes.Result{
	Fields: []*querypb.Field{
		{
			Name: "id",
			Type: querypb.Type_INT32,
		},
		{
			Name: "name",
			Type: querypb.Type_VARCHAR,
		},
	},
	Rows: [][]sqltypes.Value{
		{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("10")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice name")),
		},
		{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("20")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nicer name")),
		},
	},
}

type testHandler struct{}

func (th *testHandler) NewConnection(c *Conn) {
}

func (th *testHandler) ConnectionClosed(c *Conn) {
}

func (th *testHandler) ComQuery(c *Conn, query string) (*sqltypes.Result, error) {
	if query == "error" {
		return nil, sqldb.NewSQLError(ERUnknownComError, SSUnknownComError, "forced query handling error for: %v", query)
	}

	if query == "panic" {
		panic("test panic attack!")
	}

	if query == "select rows" {
		return selectRowsResult, nil
	}

	if query == "insert" {
		return &sqltypes.Result{
			RowsAffected: 123,
			InsertID:     123456789,
		}, nil
	}

	if query == "schema echo" {
		return &sqltypes.Result{
			Fields: []*querypb.Field{
				{
					Name: "schema_name",
					Type: querypb.Type_VARCHAR,
				},
			},
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(c.SchemaName)),
				},
			},
		}, nil
	}

	return &sqltypes.Result{}, nil
}

func TestServer(t *testing.T) {
	th := &testHandler{}

	l, err := NewListener("tcp", ":0", th)
	if err != nil {
		t.Fatalf("NewListener failed: %v", err)
	}
	defer l.Close()
	l.PasswordMap["user1"] = "password1"

	go func() {
		l.Accept()
	}()

	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port

	// Setup the right parameters.
	params := &sqldb.ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}

	// Run an 'error' command.
	output, ok := runMysql(t, params, "error")
	if ok {
		t.Fatalf("mysql should have failed: %v", output)
	}
	if !strings.Contains(output, "ERROR 1047 (08S01)") ||
		!strings.Contains(output, "forced query handling error for") {
		t.Errorf("Unexpected output for 'error'")
	}

	// Run a 'panic' command, other side should panic, recover and
	// close the connection.
	output, ok = runMysql(t, params, "panic")
	if ok {
		t.Fatalf("mysql should have failed: %v", output)
	}
	if !strings.Contains(output, "ERROR 2013 (HY000)") ||
		!strings.Contains(output, "Lost connection to MySQL server during query") {
		t.Errorf("Unexpected output for 'panic'")
	}

	// Run a 'select rows' command with results.
	output, ok = runMysql(t, params, "select rows")
	if !ok {
		t.Fatalf("mysql failed: %v", output)
	}
	if !strings.Contains(output, "nice name") ||
		!strings.Contains(output, "nicer name") ||
		!strings.Contains(output, "2 rows in set") {
		t.Errorf("Unexpected output for 'select rows'")
	}

	// Run an 'insert' command, no rows, but rows affected.
	output, ok = runMysql(t, params, "insert")
	if !ok {
		t.Fatalf("mysql failed: %v", output)
	}
	if !strings.Contains(output, "Query OK, 123 rows affected") {
		t.Errorf("Unexpected output for 'insert'")
	}

	// Run a 'schema echo' command, to make sure db name is right.
	params.DbName = "XXXfancyXXX"
	output, ok = runMysql(t, params, "schema echo")
	if !ok {
		t.Fatalf("mysql failed: %v", output)
	}
	if !strings.Contains(output, params.DbName) {
		t.Errorf("Unexpected output for 'schema echo'")
	}

	// Uncomment to leave setup up for a while, to run tests manually.
	//	fmt.Printf("Listening to server on host '%v' port '%v'.\n", host, port)
	//	time.Sleep(60 * time.Minute)
}

// runMysql forks a mysql command line process connecting to the provided server.
func runMysql(t *testing.T, params *sqldb.ConnParams, command string) (string, bool) {
	dir, err := vtenv.VtMysqlRoot()
	if err != nil {
		t.Fatalf("vtenv.VtMysqlRoot failed: %v", err)
	}
	name, err := binaryPath(dir, "mysql")
	if err != nil {
		t.Fatalf("binaryPath failed: %v", err)
	}
	// The args contain '-v' 3 times, to switch to very verbose output.
	// In particular, it has the message:
	// Query OK, 1 row affected (0.00 sec)
	args := []string{
		"-e", command,
		"-v", "-v", "-v",
	}
	if params.UnixSocket != "" {
		args = append(args, []string{
			"-S", params.UnixSocket,
		}...)
	} else {
		args = append(args, []string{
			"-h", params.Host,
			"-P", fmt.Sprintf("%v", params.Port),
		}...)
	}
	if params.Uname != "" {
		args = append(args, []string{
			"-u", params.Uname,
		}...)
	}
	if params.Pass != "" {
		args = append(args, "-p"+params.Pass)
	}
	if params.DbName != "" {
		args = append(args, []string{
			"-D", params.DbName,
		}...)
	}
	env := []string{
		"LD_LIBRARY_PATH=" + path.Join(dir, "lib/mysql"),
	}

	cmd := exec.Command(name, args...)
	cmd.Env = env
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	output := string(out)
	if err != nil {
		return output, false
	}
	return output, true
}

// binaryPath does a limited path lookup for a command,
// searching only within sbin and bin in the given root.
//
// FIXME(alainjobart) move this to vt/env, and use it from
// go/vt/mysqlctl too.
func binaryPath(root, binary string) (string, error) {
	subdirs := []string{"sbin", "bin"}
	for _, subdir := range subdirs {
		binPath := path.Join(root, subdir, binary)
		if _, err := os.Stat(binPath); err == nil {
			return binPath, nil
		}
	}
	return "", fmt.Errorf("%s not found in any of %s/{%s}",
		binary, root, strings.Join(subdirs, ","))
}
