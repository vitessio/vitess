/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"archive/zip"
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/z-division/go-zookeeper/zk"
	"golang.org/x/crypto/ssh/terminal"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/exit"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/zk2topo"
	"vitess.io/vitess/go/vt/vtctl"
)

var doc = `
zk is a tool for wrangling the zookeeper

It tries to mimic unix file system commands wherever possible, but
there are some slight differences in flag handling.

zk -h - provide help on overriding cell selection

zk addAuth digest user:pass

zk cat /zk/path
zk cat -l /zk/path1 /zk/path2 (list filename before file data)

zk chmod n-mode /zk/path
zk chmod n+mode /zk/path

zk cp /zk/path .
zk cp ./config /zk/path/config
zk cp ./config /zk/path/ (trailing slash indicates directory)

zk edit /zk/path (create a local copy, edit and write changes back to cell)

zk ls /zk
zk ls -l /zk
zk ls -ld /zk (list directory node itself)
zk ls -R /zk (recursive, expensive)

zk stat /zk/path

zk touch /zk/path
zk touch -c /zk/path (don't create, just touch timestamp)
zk touch -p /zk/path (create all parts necessary, think mkdir -p)
NOTE: there is no mkdir - just touch a node. The distinction
between file and directory is just not relevant in zookeeper.

zk rm /zk/path
zk rm -r /zk/path (recursive)
zk rm -f /zk/path (no error on nonexistent node)

zk wait /zk/path (wait for node change or creation)
zk wait /zk/path/children/ (trailing slash waits on children)

zk watch /zk/path (print changes)

zk unzip zktree.zip /
zk unzip zktree.zip /zk/prefix

zk zip /zk/root zktree.zip
NOTE: zip file can't be dumped to the file system since znodes
can have data and children.

The zk tool looks for the address of the cluster in /etc/zookeeper/zk_client.conf,
or the file specified in the ZK_CLIENT_CONFIG environment variable.

The local cell may be overridden with the ZK_CLIENT_LOCAL_CELL environment
variable.
`

const (
	timeFmt      = "2006-01-02 15:04:05"
	timeFmtMicro = "2006-01-02 15:04:05.000000"
)

type cmdFunc func(ctx context.Context, subFlags *flag.FlagSet, args []string) error

var cmdMap map[string]cmdFunc
var zconn *zk2topo.ZkConn

func init() {
	cmdMap = map[string]cmdFunc{
		"addAuth": cmdAddAuth,
		"cat":     cmdCat,
		"chmod":   cmdChmod,
		"cp":      cmdCp,
		"edit":    cmdEdit,
		"ls":      cmdLs,
		"rm":      cmdRm,
		"stat":    cmdStat,
		"touch":   cmdTouch,
		"unzip":   cmdUnzip,
		"wait":    cmdWait,
		"watch":   cmdWatch,
		"zip":     cmdZip,
	}
}

var (
	server = flag.String("server", "", "server(s) to connect to")
)

func main() {
	defer exit.Recover()
	defer logutil.Flush()
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %v:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprint(os.Stderr, doc)
	}
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		exit.Return(1)
	}

	cmdName := args[0]
	args = args[1:]
	cmd, ok := cmdMap[cmdName]
	if !ok {
		log.Exitf("Unknown command %v", cmdName)
	}
	subFlags := flag.NewFlagSet(cmdName, flag.ExitOnError)

	// Create a context for the command, cancel it if we get a signal.
	ctx, cancel := context.WithCancel(context.Background())
	sigRecv := make(chan os.Signal, 1)
	signal.Notify(sigRecv, os.Interrupt)
	go func() {
		<-sigRecv
		cancel()
	}()

	// Connect to the server.
	zconn = zk2topo.Connect(*server)

	// Run the command.
	if err := cmd(ctx, subFlags, args); err != nil {
		log.Error(err)
		exit.Return(1)
	}
}

func fixZkPath(zkPath string) string {
	if zkPath != "/" {
		zkPath = strings.TrimRight(zkPath, "/")
	}
	return path.Clean(zkPath)
}

func isZkFile(path string) bool {
	return strings.HasPrefix(path, "/zk")
}

func cmdWait(ctx context.Context, subFlags *flag.FlagSet, args []string) error {
	var (
		exitIfExists = subFlags.Bool("e", false, "exit if the path already exists")
	)

	subFlags.Parse(args)

	if subFlags.NArg() != 1 {
		return fmt.Errorf("wait: can only wait for one path")
	}
	zkPath := subFlags.Arg(0)
	isDir := zkPath[len(zkPath)-1] == '/'
	zkPath = fixZkPath(zkPath)

	var wait <-chan zk.Event
	var err error
	if isDir {
		_, _, wait, err = zconn.ChildrenW(ctx, zkPath)
	} else {
		_, _, wait, err = zconn.GetW(ctx, zkPath)
	}
	if err != nil {
		if err == zk.ErrNoNode {
			_, _, wait, _ = zconn.ExistsW(ctx, zkPath)
		} else {
			return fmt.Errorf("wait: error %v: %v", zkPath, err)
		}
	} else {
		if *exitIfExists {
			return fmt.Errorf("already exists: %v", zkPath)
		}
	}
	event := <-wait
	fmt.Printf("event: %v\n", event)
	return nil
}

// Watch for changes to the node.
func cmdWatch(ctx context.Context, subFlags *flag.FlagSet, args []string) error {
	subFlags.Parse(args)

	eventChan := make(chan zk.Event, 16)
	for _, arg := range subFlags.Args() {
		zkPath := fixZkPath(arg)
		_, _, watch, err := zconn.GetW(ctx, zkPath)
		if err != nil {
			return fmt.Errorf("watch error: %v", err)
		}
		go func() {
			eventChan <- <-watch
		}()
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-eventChan:
			log.Infof("watch: event %v: %v", event.Path, event)
			if event.Type == zk.EventNodeDataChanged {
				data, stat, watch, err := zconn.GetW(ctx, event.Path)
				if err != nil {
					return fmt.Errorf("ERROR: failed to watch %v", err)
				}
				log.Infof("watch: %v %v\n", event.Path, stat)
				println(data)
				go func() {
					eventChan <- <-watch
				}()
			} else if event.State == zk.StateDisconnected {
				return nil
			} else if event.Type == zk.EventNodeDeleted {
				log.Infof("watch: %v deleted\n", event.Path)
			} else {
				// Most likely a session event - try t
				_, _, watch, err := zconn.GetW(ctx, event.Path)
				if err != nil {
					return fmt.Errorf("ERROR: failed to watch %v", err)
				}
				go func() {
					eventChan <- <-watch
				}()
			}
		}
	}
}

func cmdLs(ctx context.Context, subFlags *flag.FlagSet, args []string) error {
	var (
		longListing      = subFlags.Bool("l", false, "long listing")
		directoryListing = subFlags.Bool("d", false, "list directory instead of contents")
		force            = subFlags.Bool("f", false, "no warning on nonexistent node")
		recursiveListing = subFlags.Bool("R", false, "recursive listing")
	)
	subFlags.Parse(args)
	if subFlags.NArg() == 0 {
		return fmt.Errorf("ls: no path specified")
	}
	resolved, err := zk2topo.ResolveWildcards(ctx, zconn, subFlags.Args())
	if err != nil {
		return fmt.Errorf("ls: invalid wildcards: %v", err)
	}
	if len(resolved) == 0 {
		// the wildcards didn't result in anything, we're
		// done.
		return nil
	}

	hasError := false
	needsHeader := len(resolved) > 1 && !*directoryListing
	for _, arg := range resolved {
		zkPath := fixZkPath(arg)
		var children []string
		var err error
		isDir := true
		if *directoryListing {
			children = []string{""}
			isDir = false
		} else if *recursiveListing {
			children, err = zk2topo.ChildrenRecursive(ctx, zconn, zkPath)
		} else {
			children, _, err = zconn.Children(ctx, zkPath)
			// Assume this is a file node if it has no children.
			if len(children) == 0 {
				children = []string{""}
				isDir = false
			}
		}
		if err != nil {
			hasError = true
			if !*force || err != zk.ErrNoNode {
				log.Warningf("ls: cannot access %v: %v", zkPath, err)
			}
		}

		// Show the full path when it helps.
		showFullPath := false
		if *recursiveListing {
			showFullPath = true
		} else if *longListing && (*directoryListing || !isDir) {
			showFullPath = true
		}
		if needsHeader {
			fmt.Printf("%v:\n", zkPath)
		}
		if len(children) > 0 {
			if *longListing && isDir {
				fmt.Printf("total: %v\n", len(children))
			}
			sort.Strings(children)
			stats := make([]*zk.Stat, len(children))
			wg := sync.WaitGroup{}
			f := func(i int) {
				localPath := path.Join(zkPath, children[i])
				_, stat, err := zconn.Exists(ctx, localPath)
				if err != nil {
					if !*force || err != zk.ErrNoNode {
						log.Warningf("ls: cannot access: %v: %v", localPath, err)
					}
				} else {
					stats[i] = stat
				}
				wg.Done()
			}
			for i := range children {
				wg.Add(1)
				go f(i)
			}
			wg.Wait()

			for i, child := range children {
				localPath := path.Join(zkPath, child)
				if stat := stats[i]; stat != nil {
					fmtPath(stat, localPath, showFullPath, *longListing)
				}
			}
		}
		if needsHeader {
			fmt.Println()
		}
	}
	if hasError {
		return fmt.Errorf("ls: some paths had errors")
	}
	return nil
}

func fmtPath(stat *zk.Stat, zkPath string, showFullPath bool, longListing bool) {
	var name, perms string

	if !showFullPath {
		name = path.Base(zkPath)
	} else {
		name = zkPath
	}

	if longListing {
		if stat.NumChildren > 0 {
			// FIXME(msolomon) do permissions check?
			perms = "drwxrwxrwx"
			if stat.DataLength > 0 {
				// give a visual indication that this node has data as well as children
				perms = "nrw-rw-rw-"
			}
		} else if stat.EphemeralOwner != 0 {
			perms = "erw-rw-rw-"
		} else {
			perms = "-rw-rw-rw-"
		}
		// always print the Local version of the time. zookeeper's
		// go / C library would return a local time anyway, but
		// might as well be sure.
		fmt.Printf("%v %v %v % 8v % 20v %v\n", perms, "zk", "zk", stat.DataLength, zk2topo.Time(stat.Mtime).Local().Format(timeFmt), name)
	} else {
		fmt.Printf("%v\n", name)
	}
}

func cmdTouch(ctx context.Context, subFlags *flag.FlagSet, args []string) error {
	var (
		createParents = subFlags.Bool("p", false, "create parents")
		touchOnly     = subFlags.Bool("c", false, "touch only - don't create")
	)

	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		return fmt.Errorf("touch: need to specify exactly one path")
	}

	zkPath := fixZkPath(subFlags.Arg(0))

	var (
		version int32 = -1
		create        = false
	)

	data, stat, err := zconn.Get(ctx, zkPath)
	switch {
	case err == nil:
		version = stat.Version
	case err == zk.ErrNoNode:
		create = true
	default:
		return fmt.Errorf("touch: cannot access %v: %v", zkPath, err)
	}

	switch {
	case !create:
		_, err = zconn.Set(ctx, zkPath, data, version)
	case *touchOnly:
		return fmt.Errorf("touch: no such path %v", zkPath)
	case *createParents:
		_, err = zk2topo.CreateRecursive(ctx, zconn, zkPath, data, 0, zk.WorldACL(zk.PermAll), 10)
	default:
		_, err = zconn.Create(ctx, zkPath, data, 0, zk.WorldACL(zk.PermAll))
	}

	if err != nil {
		return fmt.Errorf("touch: cannot modify %v: %v", zkPath, err)
	}
	return nil
}

func cmdRm(ctx context.Context, subFlags *flag.FlagSet, args []string) error {
	var (
		force             = subFlags.Bool("f", false, "no warning on nonexistent node")
		recursiveDelete   = subFlags.Bool("r", false, "recursive delete")
		forceAndRecursive = subFlags.Bool("rf", false, "shorthand for -r -f")
	)
	subFlags.Parse(args)
	*force = *force || *forceAndRecursive
	*recursiveDelete = *recursiveDelete || *forceAndRecursive

	if subFlags.NArg() == 0 {
		return fmt.Errorf("rm: no path specified")
	}

	if *recursiveDelete {
		for _, arg := range subFlags.Args() {
			zkPath := fixZkPath(arg)
			if strings.Count(zkPath, "/") < 2 {
				return fmt.Errorf("rm: overly general path: %v", zkPath)
			}
		}
	}

	resolved, err := zk2topo.ResolveWildcards(ctx, zconn, subFlags.Args())
	if err != nil {
		return fmt.Errorf("rm: invalid wildcards: %v", err)
	}
	if len(resolved) == 0 {
		// the wildcards didn't result in anything, we're done
		return nil
	}

	hasError := false
	for _, arg := range resolved {
		zkPath := fixZkPath(arg)
		var err error
		if *recursiveDelete {
			err = zk2topo.DeleteRecursive(ctx, zconn, zkPath, -1)
		} else {
			err = zconn.Delete(ctx, zkPath, -1)
		}
		if err != nil && (!*force || err != zk.ErrNoNode) {
			hasError = true
			log.Warningf("rm: cannot delete %v: %v", zkPath, err)
		}
	}
	if hasError {
		// to be consistent with the command line 'rm -f', return
		// 0 if using 'zk rm -f' and the file doesn't exist.
		return fmt.Errorf("rm: some paths had errors")
	}
	return nil
}

func cmdAddAuth(ctx context.Context, subFlags *flag.FlagSet, args []string) error {
	subFlags.Parse(args)
	if subFlags.NArg() < 2 {
		return fmt.Errorf("addAuth: expected args <scheme> <auth>")
	}
	scheme, auth := subFlags.Arg(0), subFlags.Arg(1)
	return zconn.AddAuth(ctx, scheme, []byte(auth))
}

func cmdCat(ctx context.Context, subFlags *flag.FlagSet, args []string) error {
	var (
		longListing = subFlags.Bool("l", false, "long listing")
		force       = subFlags.Bool("f", false, "no warning on nonexistent node")
		decodeProto = subFlags.Bool("p", false, "decode proto files and display them as text")
	)
	subFlags.Parse(args)
	if subFlags.NArg() == 0 {
		return fmt.Errorf("cat: no path specified")
	}
	resolved, err := zk2topo.ResolveWildcards(ctx, zconn, subFlags.Args())
	if err != nil {
		return fmt.Errorf("cat: invalid wildcards: %v", err)
	}
	if len(resolved) == 0 {
		// the wildcards didn't result in anything, we're done
		return nil
	}

	hasError := false
	for _, arg := range resolved {
		zkPath := fixZkPath(arg)
		data, _, err := zconn.Get(ctx, zkPath)
		if err != nil {
			hasError = true
			if !*force || err != zk.ErrNoNode {
				log.Warningf("cat: cannot access %v: %v", zkPath, err)
			}
			continue
		}

		if *longListing {
			fmt.Printf("%v:\n", zkPath)
		}
		decoded := ""
		if *decodeProto {
			decoded, err = vtctl.DecodeContent(zkPath, data, false)
			if err != nil {
				log.Warningf("cat: cannot proto decode %v: %v", zkPath, err)
				decoded = string(data)
			}
		} else {
			decoded = string(data)
		}
		fmt.Print(decoded)
		if len(decoded) > 0 && decoded[len(decoded)-1] != '\n' && (terminal.IsTerminal(int(os.Stdout.Fd())) || *longListing) {
			fmt.Print("\n")
		}
	}
	if hasError {
		return fmt.Errorf("cat: some paths had errors")
	}
	return nil
}

func cmdEdit(ctx context.Context, subFlags *flag.FlagSet, args []string) error {
	var (
		force = subFlags.Bool("f", false, "no warning on nonexistent node")
	)
	subFlags.Parse(args)
	if subFlags.NArg() == 0 {
		return fmt.Errorf("edit: no path specified")
	}
	arg := subFlags.Arg(0)
	zkPath := fixZkPath(arg)
	data, stat, err := zconn.Get(ctx, zkPath)
	if err != nil {
		if !*force || err != zk.ErrNoNode {
			log.Warningf("edit: cannot access %v: %v", zkPath, err)
		}
		return fmt.Errorf("edit: cannot access %v: %v", zkPath, err)
	}

	name := path.Base(zkPath)
	tmpPath := fmt.Sprintf("/tmp/zk-edit-%v-%v", name, time.Now().UnixNano())
	f, err := os.Create(tmpPath)
	if err == nil {
		_, err = f.Write(data)
		f.Close()
	}
	if err != nil {
		return fmt.Errorf("edit: cannot write file %v", err)
	}

	cmd := exec.Command(os.Getenv("EDITOR"), tmpPath)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("edit: cannot start $EDITOR: %v", err)
	}

	fileData, err := ioutil.ReadFile(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("edit: cannot read file %v", err)
	}

	if !bytes.Equal(fileData, data) {
		// data changed - update if we can
		_, err = zconn.Set(ctx, zkPath, fileData, stat.Version)
		if err != nil {
			os.Remove(tmpPath)
			return fmt.Errorf("edit: cannot write zk file %v", err)
		}
	}
	os.Remove(tmpPath)
	return nil
}

func cmdStat(ctx context.Context, subFlags *flag.FlagSet, args []string) error {
	var (
		force = subFlags.Bool("f", false, "no warning on nonexistent node")
	)
	subFlags.Parse(args)

	if subFlags.NArg() == 0 {
		return fmt.Errorf("stat: no path specified")
	}

	resolved, err := zk2topo.ResolveWildcards(ctx, zconn, subFlags.Args())
	if err != nil {
		return fmt.Errorf("stat: invalid wildcards: %v", err)
	}
	if len(resolved) == 0 {
		// the wildcards didn't result in anything, we're done
		return nil
	}

	hasError := false
	for _, arg := range resolved {
		zkPath := fixZkPath(arg)
		acls, stat, err := zconn.GetACL(ctx, zkPath)
		if stat == nil {
			err = fmt.Errorf("no such node")
		}
		if err != nil {
			hasError = true
			if !*force || err != zk.ErrNoNode {
				log.Warningf("stat: cannot access %v: %v", zkPath, err)
			}
			continue
		}
		fmt.Printf("Path: %s\n", zkPath)
		fmt.Printf("Created: %s\n", zk2topo.Time(stat.Ctime).Format(timeFmtMicro))
		fmt.Printf("Modified: %s\n", zk2topo.Time(stat.Mtime).Format(timeFmtMicro))
		fmt.Printf("Size: %v\n", stat.DataLength)
		fmt.Printf("Children: %v\n", stat.NumChildren)
		fmt.Printf("Version: %v\n", stat.Version)
		fmt.Printf("Ephemeral: %v\n", stat.EphemeralOwner)
		fmt.Printf("ACL:\n")
		for _, acl := range acls {
			fmt.Printf(" %v:%v %v\n", acl.Scheme, acl.ID, fmtACL(acl))
		}
	}
	if hasError {
		return fmt.Errorf("stat: some paths had errors")
	}
	return nil
}

var charPermMap map[string]int32
var permCharMap map[int32]string

func init() {
	charPermMap = map[string]int32{
		"r": zk.PermRead,
		"w": zk.PermWrite,
		"d": zk.PermDelete,
		"c": zk.PermCreate,
		"a": zk.PermAdmin,
	}
	permCharMap = make(map[int32]string)
	for c, p := range charPermMap {
		permCharMap[p] = c
	}
}

func fmtACL(acl zk.ACL) string {
	s := ""

	for _, perm := range []int32{zk.PermRead, zk.PermWrite, zk.PermDelete, zk.PermCreate, zk.PermAdmin} {
		if acl.Perms&perm != 0 {
			s += permCharMap[perm]
		} else {
			s += "-"
		}
	}
	return s
}

func cmdChmod(ctx context.Context, subFlags *flag.FlagSet, args []string) error {
	subFlags.Parse(args)
	if subFlags.NArg() < 2 {
		return fmt.Errorf("chmod: no permission specified")
	}
	mode := subFlags.Arg(0)
	if mode[0] != 'n' {
		return fmt.Errorf("chmod: invalid mode")
	}

	addPerms := false
	if mode[1] == '+' {
		addPerms = true
	} else if mode[1] != '-' {
		return fmt.Errorf("chmod: invalid mode")
	}

	var permMask int32
	for _, c := range mode[2:] {
		permMask |= charPermMap[string(c)]
	}

	resolved, err := zk2topo.ResolveWildcards(ctx, zconn, subFlags.Args()[1:])
	if err != nil {
		return fmt.Errorf("chmod: invalid wildcards: %v", err)
	}
	if len(resolved) == 0 {
		// the wildcards didn't result in anything, we're done
		return nil
	}

	hasError := false
	for _, arg := range resolved {
		zkPath := fixZkPath(arg)
		aclv, _, err := zconn.GetACL(ctx, zkPath)
		if err != nil {
			hasError = true
			log.Warningf("chmod: cannot set access %v: %v", zkPath, err)
			continue
		}
		if addPerms {
			aclv[0].Perms |= permMask
		} else {
			aclv[0].Perms &= ^permMask
		}
		err = zconn.SetACL(ctx, zkPath, aclv, -1)
		if err != nil {
			hasError = true
			log.Warningf("chmod: cannot set access %v: %v", zkPath, err)
			continue
		}
	}
	if hasError {
		return fmt.Errorf("chmod: some paths had errors")
	}
	return nil
}

func cmdCp(ctx context.Context, subFlags *flag.FlagSet, args []string) error {
	subFlags.Parse(args)
	switch {
	case subFlags.NArg() < 2:
		return fmt.Errorf("cp: need to specify source and destination paths")
	case subFlags.NArg() == 2:
		return fileCp(ctx, args[0], args[1])
	default:
		return multiFileCp(ctx, args)
	}
}

func getPathData(ctx context.Context, filePath string) ([]byte, error) {
	if isZkFile(filePath) {
		data, _, err := zconn.Get(ctx, filePath)
		return data, err
	}
	var err error
	file, err := os.Open(filePath)
	if err == nil {
		data, err := ioutil.ReadAll(file)
		if err == nil {
			return data, err
		}
	}
	return nil, err
}

func setPathData(ctx context.Context, filePath string, data []byte) error {
	if isZkFile(filePath) {
		_, err := zconn.Set(ctx, filePath, data, -1)
		if err == zk.ErrNoNode {
			_, err = zk2topo.CreateRecursive(ctx, zconn, filePath, data, 0, zk.WorldACL(zk.PermAll), 10)
		}
		return err
	}
	return ioutil.WriteFile(filePath, []byte(data), 0666)
}

func fileCp(ctx context.Context, srcPath, dstPath string) error {
	dstIsDir := dstPath[len(dstPath)-1] == '/'
	srcPath = fixZkPath(srcPath)
	dstPath = fixZkPath(dstPath)

	if !isZkFile(srcPath) && !isZkFile(dstPath) {
		return fmt.Errorf("cp: neither src nor dst is a /zk file: exitting")
	}

	data, err := getPathData(ctx, srcPath)
	if err != nil {
		return fmt.Errorf("cp: cannot read %v: %v", srcPath, err)
	}

	// If we are copying to a local directory - say '.', make the filename
	// the same as the source.
	if !isZkFile(dstPath) {
		fileInfo, err := os.Stat(dstPath)
		if err != nil {
			if err.(*os.PathError).Err != syscall.ENOENT {
				return fmt.Errorf("cp: cannot stat %v: %v", dstPath, err)
			}
		} else if fileInfo.IsDir() {
			dstPath = path.Join(dstPath, path.Base(srcPath))
		}
	} else if dstIsDir {
		// If we are copying into zk, interpret trailing slash as treating the
		// dstPath as a directory.
		dstPath = path.Join(dstPath, path.Base(srcPath))
	}
	if err := setPathData(ctx, dstPath, data); err != nil {
		return fmt.Errorf("cp: cannot write %v: %v", dstPath, err)
	}
	return nil
}

func multiFileCp(ctx context.Context, args []string) error {
	dstPath := args[len(args)-1]
	if dstPath[len(dstPath)-1] != '/' {
		// In multifile context, dstPath must be a directory.
		dstPath += "/"
	}

	for _, srcPath := range args[:len(args)-1] {
		if err := fileCp(ctx, srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

type zkItem struct {
	path string
	data []byte
	stat *zk.Stat
	err  error
}

// Store a zk tree in a zip archive. This won't be immediately useful to
// zip tools since even "directories" can contain data.
func cmdZip(ctx context.Context, subFlags *flag.FlagSet, args []string) error {
	subFlags.Parse(args)
	if subFlags.NArg() < 2 {
		return fmt.Errorf("zip: need to specify source and destination paths")
	}

	dstPath := subFlags.Arg(subFlags.NArg() - 1)
	paths := subFlags.Args()[:len(args)-1]
	if !strings.HasSuffix(dstPath, ".zip") {
		return fmt.Errorf("zip: need to specify destination .zip path: %v", dstPath)
	}
	zipFile, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("zip: error %v", err)
	}

	wg := sync.WaitGroup{}
	items := make(chan *zkItem, 64)
	for _, arg := range paths {
		zkPath := fixZkPath(arg)
		children, err := zk2topo.ChildrenRecursive(ctx, zconn, zkPath)
		if err != nil {
			return fmt.Errorf("zip: error %v", err)
		}
		for _, child := range children {
			toAdd := path.Join(zkPath, child)
			wg.Add(1)
			go func() {
				data, stat, err := zconn.Get(ctx, toAdd)
				items <- &zkItem{toAdd, data, stat, err}
				wg.Done()
			}()
		}
	}
	go func() {
		wg.Wait()
		close(items)
	}()

	zipWriter := zip.NewWriter(zipFile)
	for item := range items {
		path, data, stat, err := item.path, item.data, item.stat, item.err
		if err != nil {
			return fmt.Errorf("zip: get failed: %v", err)
		}
		// Skip ephemerals - not sure why you would archive them.
		if stat.EphemeralOwner > 0 {
			continue
		}
		fi := &zip.FileHeader{Name: path, Method: zip.Deflate}
		fi.Modified = zk2topo.Time(stat.Mtime)
		f, err := zipWriter.CreateHeader(fi)
		if err != nil {
			return fmt.Errorf("zip: create failed: %v", err)
		}
		_, err = f.Write(data)
		if err != nil {
			return fmt.Errorf("zip: create failed: %v", err)
		}
	}
	err = zipWriter.Close()
	if err != nil {
		return fmt.Errorf("zip: close failed: %v", err)
	}
	zipFile.Close()
	return nil
}

func cmdUnzip(ctx context.Context, subFlags *flag.FlagSet, args []string) error {
	subFlags.Parse(args)
	if subFlags.NArg() != 2 {
		return fmt.Errorf("zip: need to specify source and destination paths")
	}

	srcPath, dstPath := subFlags.Arg(0), subFlags.Arg(1)

	if !strings.HasSuffix(srcPath, ".zip") {
		return fmt.Errorf("zip: need to specify src .zip path: %v", srcPath)
	}

	zipReader, err := zip.OpenReader(srcPath)
	if err != nil {
		return fmt.Errorf("zip: error %v", err)
	}
	defer zipReader.Close()

	for _, zf := range zipReader.File {
		rc, err := zf.Open()
		if err != nil {
			return fmt.Errorf("unzip: error %v", err)
		}
		data, err := ioutil.ReadAll(rc)
		if err != nil {
			return fmt.Errorf("unzip: failed reading archive: %v", err)
		}
		zkPath := zf.Name
		if dstPath != "/" {
			zkPath = path.Join(dstPath, zkPath)
		}
		_, err = zk2topo.CreateRecursive(ctx, zconn, zkPath, data, 0, zk.WorldACL(zk.PermAll), 10)
		if err != nil && err != zk.ErrNodeExists {
			return fmt.Errorf("unzip: zk create failed: %v", err)
		}
		_, err = zconn.Set(ctx, zkPath, data, -1)
		if err != nil {
			return fmt.Errorf("unzip: zk set failed: %v", err)
		}
		rc.Close()
	}
	return nil
}
