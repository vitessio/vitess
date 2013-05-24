package main

import (
	"archive/zip"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	opts "code.google.com/p/opts-go"
	"code.google.com/p/vitess/go/terminal"
	"code.google.com/p/vitess/go/zk"

	"launchpad.net/gozk/zookeeper"
)

var zkAddrs = opts.LongSingle("--zk.addrs",
	"list of zookeeper servers (server1:port1,server2:port2,...)",
	"")
var zkoccAddr = opts.LongSingle("--zk.zkocc-addr",
	"if specified, talk to a zkocc process",
	"")
var lockWaitTimeout = opts.LongSingle("--lock-wait-timeout",
	"wait for a lock for the specified duration",
	"0")

var longListing = opts.ShortFlag("-l", "long listing")
var directoryListing = opts.ShortFlag("-d", "list directory instead of contents")
var force = opts.ShortFlag("-f", "no warning on nonexistent node")
var recursiveDelete = opts.ShortFlag("-r", "recursive delete")
var recursiveListing = opts.ShortFlag("-R", "recursive listing")
var createParents = opts.ShortFlag("-p", "create parents")
var touchOnly = opts.ShortFlag("-c", "touch only - don't create")
var exitIfExists = opts.ShortFlag("-e", "exit if the path already exists")

var doc = `zk - a tool for wrangling the zookeeper

This mimics unix file system commands wherever possible.

zk -h - provide help on overriding cell selection

zk cat /zk/path
zk cat -l /zk/path1 /zk/path2 (list filename before file data)

zk chmod n-mode /zk/path
zk chmod n+mode /zk/path

zk cp /zk/path .
zk cp ./config /zk/path/config
zk cp ./config /zk/path/ (trailing slash indicates directory)

zk edit /zk/path (create a local copy, edit and write changes back to cell)

zk elock /zk/path (create an ephemeral node that lives as long as the process)
zk qlock /zk/path/0000000001
zk qlock --lock-wait-timeout=<duration> /zk/path/0000000001

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

--zk.addrs can override the value in the conf file.
--zk.zkocc-addr can be used to connect to a zkocc process. Only a couple
  operations are then permitted (cat and ls)
`

const (
	timeFmt      = "2006-01-02 15:04:05"
	timeFmtMicro = "2006-01-02 15:04:05.000000"
)

type cmdFunc func(args []string)

var cmdMap map[string]cmdFunc
var zconn zk.Conn

func init() {
	opts.Description = doc
	cmdMap = map[string]cmdFunc{
		"cat":   cmdCat,
		"chmod": cmdChmod,
		"cp":    cmdCp,
		"edit":  cmdEdit,
		"elock": cmdElock,
		"ls":    cmdLs,
		"qlock": cmdQlock,
		"rm":    cmdRm,
		"stat":  cmdStat,
		"touch": cmdTouch,
		"unzip": cmdUnzip,
		"wait":  cmdWait,
		"watch": cmdWatch,
		"zip":   cmdZip,
	}
	log.SetFlags(0)

	zconn = zk.NewMetaConn(false)
}

func main() {
	opts.Parse()
	args := opts.Args
	if len(args) == 0 {
		opts.Help()
		os.Exit(1)
	}

	if *zkAddrs != "" {
		if *zkoccAddr != "" {
			log.Fatalf("zk.addrs and zk.zkocc-addr are mutually exclusive")
		}
		var err error
		zconn, _, err = zk.DialZkTimeout(*zkAddrs, 5*time.Second, 10*time.Second)
		if err != nil {
			log.Fatalf("zk connect failed: %v", err.Error())
		}
	}

	if *zkoccAddr != "" {
		var err error
		zconn, err = zk.DialZkocc(*zkoccAddr, 5*time.Second)
		if err != nil {
			log.Fatalf("zkocc connect failed: %v", err.Error())
		}
	}

	cmdName := args[0]
	args = args[1:]
	if cmd, ok := cmdMap[cmdName]; ok {
		cmd(args)
	} else {
		opts.Help()
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

func cmdWait(args []string) {
	if len(args) != 1 {
		log.Fatalf("wait: can only wait for one path")
	}
	zkPath := args[0]
	isDir := zkPath[len(zkPath)-1] == '/'
	zkPath = fixZkPath(zkPath)

	var wait <-chan zookeeper.Event
	var err error
	if isDir {
		_, _, wait, err = zconn.ChildrenW(zkPath)
	} else {
		_, _, wait, err = zconn.GetW(zkPath)
	}
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			_, wait, err = zconn.ExistsW(zkPath)
		} else {
			log.Fatalf("wait: error %v: %v", zkPath, err)
		}
	} else {
		if *exitIfExists {
			fmt.Printf("already exists: %v\n", zkPath)
			return
		}
	}
	event := <-wait
	fmt.Printf("event: %v\n", event)
}

func cmdQlock(args []string) {
	zkPath := fixZkPath(args[0])
	timeout, err := time.ParseDuration(*lockWaitTimeout)
	if err != nil {
		log.Fatalf("qlock: invalid timeout %v: %v", *lockWaitTimeout, err)
	}
	sigRecv := make(chan os.Signal, 1)
	interrupted := make(chan struct{})
	signal.Notify(sigRecv, os.Interrupt)
	go func() {
		<-sigRecv
		close(interrupted)
	}()
	err = zk.ObtainQueueLock(zconn, zkPath, timeout, interrupted)
	if err != nil {
		log.Fatalf("qlock: error %v: %v", zkPath, err)
	}
	fmt.Printf("qlock: locked %v\n", zkPath)
}

// Create an ephemeral node an just wait.
func cmdElock(args []string) {
	zkPath := fixZkPath(args[0])
	// Speed up case where we die nicely, otherwise you have to wait for
	// the server to notice the client's demise.
	sigRecv := make(chan os.Signal, 1)
	signal.Notify(sigRecv, os.Interrupt)

	for {
		_, err := zconn.Create(zkPath, "", zookeeper.EPHEMERAL, zookeeper.WorldACL(zookeeper.PERM_ALL))
		if err != nil {
			log.Fatalf("elock: error %v: %v", zkPath, err)
		}

	watchLoop:
		for {
			_, _, watch, err := zconn.GetW(zkPath)
			if err != nil {
				log.Fatalf("elock: error %v: %v", zkPath, err)
			}
			select {
			case <-sigRecv:
				zconn.Delete(zkPath, -1)
				return
			case event := <-watch:
				log.Printf("elock: event %v: %v", zkPath, event)
				if !event.Ok() {
					//log.Fatalf("elock: error %v: %v", zkPath, event)
					break watchLoop
				}
			}
		}
	}
}

// Watch for changes to the node.
func cmdWatch(args []string) {
	// Speed up case where we die nicely, otherwise you have to wait for
	// the server to notice the client's demise.
	sigRecv := make(chan os.Signal, 1)
	signal.Notify(sigRecv, os.Interrupt)

	eventChan := make(chan zookeeper.Event, 16)
	for _, arg := range args {
		zkPath := fixZkPath(arg)
		_, _, watch, err := zconn.GetW(zkPath)
		if err != nil {
			log.Fatalf("watch error: %v", err)
		}
		go func() {
			eventChan <- <-watch
		}()
	}

	for {
		select {
		case <-sigRecv:
			return
		case event := <-eventChan:
			log.Printf("watch: event %v: %v", event.Path, event)
			if event.Type == zookeeper.EVENT_CHANGED {
				data, stat, watch, err := zconn.GetW(event.Path)
				if err != nil {
					log.Fatalf("ERROR: failed to watch %v", err)
				}
				log.Printf("watch: %v %v\n", event.Path, stat)
				println(data)
				go func() {
					eventChan <- <-watch
				}()
			} else if event.State == zookeeper.STATE_CLOSED {
				return
			} else if event.Type == zookeeper.EVENT_DELETED {
				log.Printf("watch: %v deleted\n", event.Path)
			} else {
				// Most likely a session event - try t
				_, _, watch, err := zconn.GetW(event.Path)
				if err != nil {
					log.Fatalf("ERROR: failed to watch %v", err)
				}
				go func() {
					eventChan <- <-watch
				}()
			}
		}
	}
}

func cmdLs(args []string) {
	if len(args) == 0 {
		log.Fatal("ls: no path specified")
	}
	args, err := zk.ResolveWildcards(zconn, args)
	if err != nil {
		log.Fatalf("ls: invalid wildcards: %v", err)
	}
	if len(args) == 0 {
		// the wildcards didn't result in anything, we're done
		return
	}

	hasError := false
	needsHeader := len(args) > 1 && !*directoryListing
	for _, arg := range args {
		zkPath := fixZkPath(arg)
		var children []string
		var err error
		isDir := true
		if *directoryListing {
			children = []string{""}
			isDir = false
		} else if *recursiveListing {
			children, err = zk.ChildrenRecursive(zconn, zkPath)
		} else {
			children, _, err = zconn.Children(zkPath)
			// Assume this is a file node if it has no children.
			if len(children) == 0 {
				children = []string{""}
				isDir = false
			}
		}
		if err != nil {
			hasError = true
			if !*force || !zookeeper.IsError(err, zookeeper.ZNONODE) {
				log.Printf("ls: cannot access %v: %v", zkPath, err)
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
			stats := make([]zk.Stat, len(children))
			wg := sync.WaitGroup{}
			f := func(i int) {
				localPath := path.Join(zkPath, children[i])
				stat, err := zconn.Exists(localPath)
				if err != nil {
					if !*force || !zookeeper.IsError(err, zookeeper.ZNONODE) {
						log.Printf("ls: cannot access: %v: %v", localPath, err)
					}
				} else {
					stats[i] = stat
				}
				wg.Done()
			}
			for i, _ := range children {
				wg.Add(1)
				go f(i)
			}
			wg.Wait()

			for i, child := range children {
				localPath := path.Join(zkPath, child)
				if stat := stats[i]; stat != nil {
					fmtPath(stat, localPath, showFullPath)
				}
			}
		}
		if needsHeader {
			fmt.Println()
		}
	}
	if hasError {
		os.Exit(1)
	}
}

func fmtPath(stat zk.Stat, zkPath string, showFullPath bool) {
	var name, perms string

	if !showFullPath {
		name = path.Base(zkPath)
	} else {
		name = zkPath
	}

	if *longListing {
		if stat.NumChildren() > 0 {
			// FIXME(msolomon) do permissions check?
			perms = "drwxrwxrwx"
			if stat.DataLength() > 0 {
				// give a visual indication that this node has data as well as children
				perms = "nrw-rw-rw-"
			}
		} else if stat.EphemeralOwner() != 0 {
			perms = "erw-rw-rw-"
		} else {
			perms = "-rw-rw-rw-"
		}
		// always print the Local version of the time. zookeeper's
		// go / C library would return a local time, whereas
		// gorpc to zkocc returns a UTC time. By always printing the
		// Local version we make them the same.
		fmt.Printf("%v %v %v % 8v % 20v %v\n", perms, "zk", "zk", stat.DataLength(), stat.MTime().Local().Format(timeFmt), name)
	} else {
		fmt.Printf("%v\n", name)
	}
}

func cmdTouch(args []string) {
	if len(args) != 1 {
		log.Fatal("touch: need to specify exactly one path")
	}

	zkPath := fixZkPath(args[0])
	if !isZkFile(zkPath) {
		log.Fatalf("touch: not a /zk file %v", zkPath)
	}

	data, stat, err := zconn.Get(zkPath)
	version := -1
	create := false
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			create = true
		} else {
			log.Fatalf("touch: cannot access %v: %v", zkPath, err)
		}
	} else {
		version = stat.Version()
	}

	if create {
		if *touchOnly {
			log.Fatalf("touch: no such path %v", zkPath)
		}
		if *createParents {
			_, err = zk.CreateRecursive(zconn, zkPath, data, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
		} else {
			_, err = zconn.Create(zkPath, data, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
		}
	} else {
		_, err = zconn.Set(zkPath, data, version)
	}

	if err != nil {
		log.Fatalf("touch: cannot modify %v: %v", zkPath, err)
	}
}

func cmdRm(args []string) {
	if len(args) == 0 {
		log.Fatal("rm: no path specified")
	}

	if *recursiveDelete {
		for _, arg := range args {
			zkPath := fixZkPath(arg)
			if strings.Count(zkPath, "/") < 4 {
				log.Fatalf("rm: overly general path: %v", zkPath)
			}
		}
	}

	args, err := zk.ResolveWildcards(zconn, args)
	if err != nil {
		log.Fatalf("rm: invalid wildcards: %v", err)
	}
	if len(args) == 0 {
		// the wildcards didn't result in anything, we're done
		return
	}

	hasError := false
	for _, arg := range args {
		zkPath := fixZkPath(arg)
		var err error
		if *recursiveDelete {
			err = zk.DeleteRecursive(zconn, zkPath, -1)
		} else {
			err = zconn.Delete(zkPath, -1)
		}
		if err != nil {
			if !*force || !zookeeper.IsError(err, zookeeper.ZNONODE) {
				hasError = true
				log.Printf("rm: cannot delete %v: %v", zkPath, err)
			}
		}
	}
	if hasError {
		// to be consistent with the command line 'rm -f', return
		// 0 if using 'zk rm -f' and the file doesn't exist.
		os.Exit(1)
	}
}

func cmdCat(args []string) {
	if len(args) == 0 {
		log.Fatal("cat: no path specified")
	}
	args, err := zk.ResolveWildcards(zconn, args)
	if err != nil {
		log.Fatalf("cat: invalid wildcards: %v", err)
	}
	if len(args) == 0 {
		// the wildcards didn't result in anything, we're done
		return
	}

	hasError := false
	for _, arg := range args {
		zkPath := fixZkPath(arg)
		data, _, err := zconn.Get(zkPath)
		if err != nil {
			hasError = true
			if !*force || !zookeeper.IsError(err, zookeeper.ZNONODE) {
				log.Printf("cat: cannot access %v: %v", zkPath, err)
			}
		} else {
			if *longListing {
				fmt.Printf("%v:\n", zkPath)
			}
			fmt.Print(data)
			if len(data) > 0 && data[len(data)-1] != '\n' && (terminal.IsTerminal(os.Stdout.Fd()) || *longListing) {
				fmt.Print("\n")
			}
		}
	}
	if hasError {
		os.Exit(1)
	}
}

func cmdEdit(args []string) {
	if len(args) == 0 {
		log.Fatal("edit: no path specified")
	}
	arg := args[0]
	zkPath := fixZkPath(arg)
	data, stat, err := zconn.Get(zkPath)
	if err != nil {
		if !*force || !zookeeper.IsError(err, zookeeper.ZNONODE) {
			log.Printf("edit: cannot access %v: %v", zkPath, err)
		}
		os.Exit(1)
	}

	name := path.Base(zkPath)
	tmpPath := fmt.Sprintf("/tmp/zk-edit-%v-%v", name, time.Now().UnixNano())
	f, err := os.Create(tmpPath)
	if err == nil {
		_, err = f.WriteString(data)
		f.Close()
	}
	if err != nil {
		log.Printf("edit: cannot write file %v", err)
		os.Exit(1)
	}

	cmd := exec.Command(os.Getenv("EDITOR"), tmpPath)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		os.Remove(tmpPath)
		log.Printf("edit: cannot start $EDITOR: %v", err)
		os.Exit(1)
	}

	fileData, err := ioutil.ReadFile(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		log.Printf("edit: cannot read file %v", err)
		os.Exit(1)
	}

	if string(fileData) != data {
		// data changed - update if we can
		_, err = zconn.Set(zkPath, string(fileData), stat.Version())
		if err != nil {
			os.Remove(tmpPath)
			log.Printf("edit: cannot write zk file %v", err)
			os.Exit(1)
		}
	}
	os.Remove(tmpPath)
}

func cmdStat(args []string) {
	if len(args) == 0 {
		log.Fatal("stat: no path specified")
	}
	args, err := zk.ResolveWildcards(zconn, args)
	if err != nil {
		log.Fatalf("stat: invalid wildcards: %v", err)
	}
	if len(args) == 0 {
		// the wildcards didn't result in anything, we're done
		return
	}

	hasError := false
	for _, arg := range args {
		zkPath := fixZkPath(arg)
		acls, stat, err := zconn.ACL(zkPath)
		if stat == nil {
			err = fmt.Errorf("no such node")
		}
		if err != nil {
			hasError = true
			if !*force || !zookeeper.IsError(err, zookeeper.ZNONODE) {
				log.Printf("stat: cannot access %v: %v", zkPath, err)
			}
			continue
		}
		fmt.Printf("Path: %s\n", zkPath)
		fmt.Printf("Created: %s\n", stat.CTime().Format(timeFmtMicro))
		fmt.Printf("Modified: %s\n", stat.MTime().Format(timeFmtMicro))
		fmt.Printf("Size: %v\n", stat.DataLength())
		fmt.Printf("Children: %v\n", stat.NumChildren())
		fmt.Printf("Version: %v\n", stat.Version())
		fmt.Printf("Ephemeral: %v\n", stat.EphemeralOwner())
		fmt.Printf("ACL:\n")
		for _, acl := range acls {
			fmt.Printf(" %v:%v %v\n", acl.Scheme, acl.Id, fmtAcl(acl))
		}
	}
	if hasError {
		os.Exit(1)
	}
}

var charPermMap map[string]uint32
var permCharMap map[uint32]string

func init() {
	charPermMap = map[string]uint32{
		"r": zookeeper.PERM_READ,
		"w": zookeeper.PERM_WRITE,
		"d": zookeeper.PERM_DELETE,
		"c": zookeeper.PERM_CREATE,
		"a": zookeeper.PERM_ADMIN,
	}
	permCharMap = make(map[uint32]string)
	for c, p := range charPermMap {
		permCharMap[p] = c
	}
}

func fmtAcl(acl zookeeper.ACL) string {
	s := ""

	for _, perm := range []uint32{zookeeper.PERM_READ, zookeeper.PERM_WRITE, zookeeper.PERM_DELETE, zookeeper.PERM_CREATE, zookeeper.PERM_ADMIN} {
		if acl.Perms&perm != 0 {
			s += permCharMap[perm]
		} else {
			s += "-"
		}
	}
	return s
}

func cmdChmod(args []string) {
	if len(args) < 2 {
		log.Fatal("chmod: no permission specified")
	}
	mode := args[0]
	if mode[0] != 'n' {
		log.Fatal("chmod: invalid mode")
	}

	addPerms := false
	if mode[1] == '+' {
		addPerms = true
	} else if mode[1] != '-' {
		log.Fatal("chmod: invalid mode")
	}

	var permMask uint32
	for _, c := range mode[2:] {
		permMask |= charPermMap[string(c)]
	}

	args, err := zk.ResolveWildcards(zconn, args[1:])
	if err != nil {
		log.Fatalf("chmod: invalid wildcards: %v", err)
	}
	if len(args) == 0 {
		// the wildcards didn't result in anything, we're done
		return
	}

	hasError := false
	for _, arg := range args {
		zkPath := fixZkPath(arg)
		aclv, _, err := zconn.ACL(zkPath)
		if err != nil {
			hasError = true
			log.Printf("chmod: cannot set access %v: %v", zkPath, err)
			continue
		}
		if addPerms {
			aclv[0].Perms |= permMask
		} else {
			aclv[0].Perms &= ^permMask
		}
		err = zconn.SetACL(zkPath, aclv, -1)
		if err != nil {
			hasError = true
			log.Printf("chmod: cannot set access %v: %v", zkPath, err)
			continue
		}
	}
	if hasError {
		os.Exit(1)
	}
}

func cmdCp(args []string) {
	if len(args) < 2 {
		log.Fatalf("cp: need to specify source and destination paths")
	} else if len(args) == 2 {
		fileCp(args[0], args[1])
	} else {
		multiFileCp(args)
	}
}

func getPathData(filePath string) (string, error) {
	if isZkFile(filePath) {
		data, _, err := zconn.Get(filePath)
		return data, err
	} else {
		var err error
		file, err := os.Open(filePath)
		if err == nil {
			data, err := ioutil.ReadAll(file)
			if err == nil {
				return string(data), err
			}
		}
		return "", err
	}
	panic("unreachable")
}

func setPathData(filePath, data string) error {
	if isZkFile(filePath) {
		_, err := zconn.Set(filePath, data, -1)
		if err != nil && zookeeper.IsError(err, zookeeper.ZNONODE) {
			_, err = zk.CreateRecursive(zconn, filePath, data, 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
		}
		return err
	} else {
		return ioutil.WriteFile(filePath, []byte(data), 0666)
	}
	panic("unreachable")
}

func fileCp(srcPath, dstPath string) {
	dstIsDir := dstPath[len(dstPath)-1] == '/'
	srcPath = fixZkPath(srcPath)
	dstPath = fixZkPath(dstPath)

	if !isZkFile(srcPath) && !isZkFile(dstPath) {
		log.Fatal("cp: neither src nor dst is a /zk file: exitting")
	}

	data, err := getPathData(srcPath)
	if err != nil {
		log.Fatalf("cp: cannot read %v: %v", srcPath, err)
	}

	// If we are copying to a local directory - say '.', make the filename
	// the same as the source.
	if !isZkFile(dstPath) {
		fileInfo, err := os.Stat(dstPath)
		if err != nil {
			if err.(*os.PathError).Err != syscall.ENOENT {
				log.Fatalf("cp: cannot stat %v: %v", dstPath, err)
			}
		} else if fileInfo.IsDir() {
			dstPath = path.Join(dstPath, path.Base(srcPath))
		}
	} else if dstIsDir {
		// If we are copying into zk, interpret trailing slash as treating the
		// dstPath as a directory.
		dstPath = path.Join(dstPath, path.Base(srcPath))
	}
	if err := setPathData(dstPath, data); err != nil {
		log.Fatalf("cp: cannot write %v: %v", dstPath, err)
	}
}

func multiFileCp(args []string) {
	dstPath := args[len(args)-1]
	if dstPath[len(dstPath)-1] != '/' {
		// In multifile context, dstPath must be a directory.
		dstPath += "/"
	}

	for _, srcPath := range args[:len(args)-1] {
		fileCp(srcPath, dstPath)
	}
}

type zkItem struct {
	path string
	data string
	stat zk.Stat
	err  error
}

// Store a zk tree in a zip archive. This won't be immediately useful to
// zip tools since even "directories" can contain data.
func cmdZip(args []string) {
	if len(args) < 2 {
		log.Fatalf("zip: need to specify source and destination paths")
	}

	dstPath := args[len(args)-1]
	args = args[:len(args)-1]
	if !strings.HasSuffix(dstPath, ".zip") {
		log.Fatalf("zip: need to specify destination .zip path: %v", dstPath)
	}

	zipFile, err := os.Create(dstPath)
	if err != nil {
		log.Fatalf("zip: error %v", err)
	}

	wg := sync.WaitGroup{}
	items := make(chan *zkItem, 64)
	for _, arg := range args {
		zkPath := fixZkPath(arg)
		children, err := zk.ChildrenRecursive(zconn, zkPath)
		if err != nil {
			log.Fatalf("zip: error %v", err)
		}
		for _, child := range children {
			toAdd := path.Join(zkPath, child)
			wg.Add(1)
			go func() {
				data, stat, err := zconn.Get(toAdd)
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
			log.Fatal("zip: get failed: %v", err)
		}
		// Skip ephemerals - not sure why you would archive them.
		if stat.EphemeralOwner() > 0 {
			continue
		}
		fi := &zip.FileHeader{Name: path, Method: zip.Deflate}
		fi.SetModTime(stat.MTime())
		f, err := zipWriter.CreateHeader(fi)
		if err != nil {
			log.Fatal("zip: create failed: %v", err)
		}
		_, err = f.Write([]byte(data))
		if err != nil {
			log.Fatal("zip: create failed: %v", err)
		}
	}
	err = zipWriter.Close()
	if err != nil {
		log.Fatal("zip: close failed: %v", err)
	}
	zipFile.Close()
}

func cmdUnzip(args []string) {
	if len(args) != 2 {
		log.Fatalf("zip: need to specify source and destination paths")
	}

	srcPath := args[0]
	dstPath := args[1]

	if !strings.HasSuffix(srcPath, ".zip") {
		log.Fatalf("zip: need to specify src .zip path: %v", srcPath)
	}

	zipReader, err := zip.OpenReader(srcPath)
	if err != nil {
		log.Fatalf("zip: error %v", err)
	}
	defer zipReader.Close()

	for _, zf := range zipReader.File {
		rc, err := zf.Open()
		if err != nil {
			log.Fatalf("unzip: error %v", err)
		}
		data, err := ioutil.ReadAll(rc)
		if err != nil {
			log.Fatal("unzip: failed reading archive: %v", err)
		}
		zkPath := zf.Name
		if dstPath != "/" {
			zkPath = path.Join(dstPath, zkPath)
		}
		_, err = zk.CreateRecursive(zconn, zkPath, string(data), 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
		if err != nil && !zookeeper.IsError(err, zookeeper.ZNODEEXISTS) {
			log.Fatal("unzip: zk create failed: %v", err)
		}
		_, err = zconn.Set(zkPath, string(data), -1)
		if err != nil {
			log.Fatal("unzip: zk set failed: %v", err)
		}
		rc.Close()
	}
}
