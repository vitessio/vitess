package main

import (
	"archive/zip"
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

	log "github.com/golang/glog"
	zookeeper "github.com/samuel/go-zookeeper/zk"

	"github.com/youtube/vitess/go/exit"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/zk"
	"golang.org/x/crypto/ssh/terminal"
)

var doc = `
zk is a tool for wrangling the zookeeper

It tries to mimic unix file system commands wherever possible, but
there are some slight differences in flag handling.

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
`

const (
	timeFmt      = "2006-01-02 15:04:05"
	timeFmtMicro = "2006-01-02 15:04:05.000000"
)

type cmdFunc func(subFlags *flag.FlagSet, args []string) error

var cmdMap map[string]cmdFunc
var zconn zk.Conn

func init() {
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

	zconn = zk.NewMetaConn()
}

var (
	zkAddrs = flag.String("zk.addrs", "", "list of zookeeper servers (server1:port1,server2:port2,...) which overrides the conf file")
)

func main() {
	defer exit.Recover()
	defer logutil.Flush()
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %v:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, doc)
	}
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		exit.Return(1)
	}

	if *zkAddrs != "" {
		var err error
		zconn, _, err = zk.DialZkTimeout(*zkAddrs, 5*time.Second, 10*time.Second)
		if err != nil {
			log.Errorf("zk connect failed: %v", err.Error())
			exit.Return(1)
		}
	}

	cmdName := args[0]
	args = args[1:]
	if cmd, ok := cmdMap[cmdName]; ok {
		subFlags := flag.NewFlagSet(cmdName, flag.ExitOnError)
		if err := cmd(subFlags, args); err != nil {
			log.Error(err)
			exit.Return(1)
		}
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

func cmdWait(subFlags *flag.FlagSet, args []string) error {
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

	var wait <-chan zookeeper.Event
	var err error
	if isDir {
		_, _, wait, err = zconn.ChildrenW(zkPath)
	} else {
		_, _, wait, err = zconn.GetW(zkPath)
	}
	if err != nil {
		if err == zookeeper.ErrNoNode {
			_, wait, err = zconn.ExistsW(zkPath)
		} else {
			return fmt.Errorf("wait: error %v: %v", zkPath, err)
		}
	} else {
		if *exitIfExists {
			return fmt.Errorf("already exists: %v\n", zkPath)
		}
	}
	event := <-wait
	fmt.Printf("event: %v\n", event)
	return nil
}

func cmdQlock(subFlags *flag.FlagSet, args []string) error {
	var (
		lockWaitTimeout = subFlags.Duration("lock-wait-timeout", 0, "wait for a lock for the specified duration")
	)
	subFlags.Parse(args)
	zkPath := fixZkPath(subFlags.Arg(0))
	sigRecv := make(chan os.Signal, 1)
	interrupted := make(chan struct{})
	signal.Notify(sigRecv, os.Interrupt)
	go func() {
		<-sigRecv
		close(interrupted)
	}()
	if err := zk.ObtainQueueLock(zconn, zkPath, *lockWaitTimeout, interrupted); err != nil {
		return fmt.Errorf("qlock: error %v: %v", zkPath, err)
	}
	fmt.Printf("qlock: locked %v\n", zkPath)
	return nil
}

// Create an ephemeral node an just wait.
func cmdElock(subFlags *flag.FlagSet, args []string) error {
	subFlags.Parse(args)
	zkPath := fixZkPath(subFlags.Arg(0))
	// Speed up case where we die nicely, otherwise you have to wait for
	// the server to notice the client's demise.
	sigRecv := make(chan os.Signal, 1)
	signal.Notify(sigRecv, os.Interrupt)

	for {
		_, err := zconn.Create(zkPath, "", zookeeper.FlagEphemeral, zookeeper.WorldACL(zookeeper.PermAll))
		if err != nil {
			return fmt.Errorf("elock: error %v: %v", zkPath, err)
		}

	watchLoop:
		for {
			_, _, watch, err := zconn.GetW(zkPath)
			if err != nil {
				return fmt.Errorf("elock: error %v: %v", zkPath, err)
			}
			select {
			case <-sigRecv:
				zconn.Delete(zkPath, -1)
				return nil
			case event := <-watch:
				log.Infof("elock: event %v: %v", zkPath, event)
				if event.Err != nil {
					//return fmt.Errorf("elock: error %v: %v", zkPath, event)
					break watchLoop
				}
			}
		}
	}
}

// Watch for changes to the node.
func cmdWatch(subFlags *flag.FlagSet, args []string) error {
	subFlags.Parse(args)
	// Speed up case where we die nicely, otherwise you have to wait for
	// the server to notice the client's demise.
	sigRecv := make(chan os.Signal, 1)
	signal.Notify(sigRecv, os.Interrupt)

	eventChan := make(chan zookeeper.Event, 16)
	for _, arg := range subFlags.Args() {
		zkPath := fixZkPath(arg)
		_, _, watch, err := zconn.GetW(zkPath)
		if err != nil {
			return fmt.Errorf("watch error: %v", err)
		}
		go func() {
			eventChan <- <-watch
		}()
	}

	for {
		select {
		case <-sigRecv:
			return nil
		case event := <-eventChan:
			log.Infof("watch: event %v: %v", event.Path, event)
			if event.Type == zookeeper.EventNodeDataChanged {
				data, stat, watch, err := zconn.GetW(event.Path)
				if err != nil {
					return fmt.Errorf("ERROR: failed to watch %v", err)
				}
				log.Infof("watch: %v %v\n", event.Path, stat)
				println(data)
				go func() {
					eventChan <- <-watch
				}()
			} else if event.State == zookeeper.StateDisconnected {
				return nil
			} else if event.Type == zookeeper.EventNodeDeleted {
				log.Infof("watch: %v deleted\n", event.Path)
			} else {
				// Most likely a session event - try t
				_, _, watch, err := zconn.GetW(event.Path)
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

func cmdLs(subFlags *flag.FlagSet, args []string) error {
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
	// FIXME(szopa): shadowing?
	resolved, err := zk.ResolveWildcards(zconn, subFlags.Args())
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
			if !*force || err != zookeeper.ErrNoNode {
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
			stats := make([]*zookeeper.Stat, len(children))
			wg := sync.WaitGroup{}
			f := func(i int) {
				localPath := path.Join(zkPath, children[i])
				stat, err := zconn.Exists(localPath)
				if err != nil {
					if !*force || err != zookeeper.ErrNoNode {
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

func fmtPath(stat *zookeeper.Stat, zkPath string, showFullPath bool, longListing bool) {
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
		fmt.Printf("%v %v %v % 8v % 20v %v\n", perms, "zk", "zk", stat.DataLength, zk.Time(stat.Mtime).Local().Format(timeFmt), name)
	} else {
		fmt.Printf("%v\n", name)
	}
}

func cmdTouch(subFlags *flag.FlagSet, args []string) error {
	var (
		createParents = subFlags.Bool("p", false, "create parents")
		touchOnly     = subFlags.Bool("c", false, "touch only - don't create")
	)

	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		return fmt.Errorf("touch: need to specify exactly one path")
	}

	zkPath := fixZkPath(subFlags.Arg(0))
	if !isZkFile(zkPath) {
		return fmt.Errorf("touch: not a /zk file %v", zkPath)
	}

	var (
		version int32 = -1
		create        = false
	)

	data, stat, err := zconn.Get(zkPath)
	switch {
	case err == nil:
		version = stat.Version
	case err == zookeeper.ErrNoNode:
		create = true
	default:
		return fmt.Errorf("touch: cannot access %v: %v", zkPath, err)
	}

	switch {
	case !create:
		_, err = zconn.Set(zkPath, data, version)
	case *touchOnly:
		return fmt.Errorf("touch: no such path %v", zkPath)
	case *createParents:
		_, err = zk.CreateRecursive(zconn, zkPath, data, 0, zookeeper.WorldACL(zookeeper.PermAll))
	default:
		_, err = zconn.Create(zkPath, data, 0, zookeeper.WorldACL(zookeeper.PermAll))
	}

	if err != nil {
		return fmt.Errorf("touch: cannot modify %v: %v", zkPath, err)
	}
	return nil
}

func cmdRm(subFlags *flag.FlagSet, args []string) error {
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
			if strings.Count(zkPath, "/") < 4 {
				return fmt.Errorf("rm: overly general path: %v", zkPath)
			}
		}
	}

	resolved, err := zk.ResolveWildcards(zconn, subFlags.Args())
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
			err = zk.DeleteRecursive(zconn, zkPath, -1)
		} else {
			err = zconn.Delete(zkPath, -1)
		}
		if err != nil && (!*force || err != zookeeper.ErrNoNode) {
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

func cmdCat(subFlags *flag.FlagSet, args []string) error {
	var (
		longListing = subFlags.Bool("l", false, "long listing")
		force       = subFlags.Bool("f", false, "no warning on nonexistent node")
	)
	subFlags.Parse(args)
	if subFlags.NArg() == 0 {
		return fmt.Errorf("cat: no path specified")
	}
	resolved, err := zk.ResolveWildcards(zconn, subFlags.Args())
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
		data, _, err := zconn.Get(zkPath)
		if err != nil {
			hasError = true
			if !*force || err != zookeeper.ErrNoNode {
				log.Warningf("cat: cannot access %v: %v", zkPath, err)
			}
		} else {
			if *longListing {
				fmt.Printf("%v:\n", zkPath)
			}
			fmt.Print(data)
			if len(data) > 0 && data[len(data)-1] != '\n' && (terminal.IsTerminal(int(os.Stdout.Fd())) || *longListing) {
				fmt.Print("\n")
			}
		}
	}
	if hasError {
		return fmt.Errorf("cat: some paths had errors")
	}
	return nil
}

func cmdEdit(subFlags *flag.FlagSet, args []string) error {
	var (
		force = subFlags.Bool("f", false, "no warning on nonexistent node")
	)
	subFlags.Parse(args)
	if subFlags.NArg() == 0 {
		return fmt.Errorf("edit: no path specified")
	}
	arg := subFlags.Arg(0)
	zkPath := fixZkPath(arg)
	data, stat, err := zconn.Get(zkPath)
	if err != nil {
		if !*force || err != zookeeper.ErrNoNode {
			log.Warningf("edit: cannot access %v: %v", zkPath, err)
		}
		return fmt.Errorf("edit: cannot access %v: %v", zkPath, err)
	}

	name := path.Base(zkPath)
	tmpPath := fmt.Sprintf("/tmp/zk-edit-%v-%v", name, time.Now().UnixNano())
	f, err := os.Create(tmpPath)
	if err == nil {
		_, err = f.WriteString(data)
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

	if string(fileData) != data {
		// data changed - update if we can
		_, err = zconn.Set(zkPath, string(fileData), stat.Version)
		if err != nil {
			os.Remove(tmpPath)
			return fmt.Errorf("edit: cannot write zk file %v", err)
		}
	}
	os.Remove(tmpPath)
	return nil
}

func cmdStat(subFlags *flag.FlagSet, args []string) error {
	var (
		force = subFlags.Bool("f", false, "no warning on nonexistent node")
	)
	subFlags.Parse(args)

	if subFlags.NArg() == 0 {
		return fmt.Errorf("stat: no path specified")
	}

	resolved, err := zk.ResolveWildcards(zconn, subFlags.Args())
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
		acls, stat, err := zconn.ACL(zkPath)
		if stat == nil {
			err = fmt.Errorf("no such node")
		}
		if err != nil {
			hasError = true
			if !*force || err != zookeeper.ErrNoNode {
				log.Warningf("stat: cannot access %v: %v", zkPath, err)
			}
			continue
		}
		fmt.Printf("Path: %s\n", zkPath)
		fmt.Printf("Created: %s\n", zk.Time(stat.Ctime).Format(timeFmtMicro))
		fmt.Printf("Modified: %s\n", zk.Time(stat.Mtime).Format(timeFmtMicro))
		fmt.Printf("Size: %v\n", stat.DataLength)
		fmt.Printf("Children: %v\n", stat.NumChildren)
		fmt.Printf("Version: %v\n", stat.Version)
		fmt.Printf("Ephemeral: %v\n", stat.EphemeralOwner)
		fmt.Printf("ACL:\n")
		for _, acl := range acls {
			fmt.Printf(" %v:%v %v\n", acl.Scheme, acl.ID, fmtAcl(acl))
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
		"r": zookeeper.PermRead,
		"w": zookeeper.PermWrite,
		"d": zookeeper.PermDelete,
		"c": zookeeper.PermCreate,
		"a": zookeeper.PermAdmin,
	}
	permCharMap = make(map[int32]string)
	for c, p := range charPermMap {
		permCharMap[p] = c
	}
}

func fmtAcl(acl zookeeper.ACL) string {
	s := ""

	for _, perm := range []int32{zookeeper.PermRead, zookeeper.PermWrite, zookeeper.PermDelete, zookeeper.PermCreate, zookeeper.PermAdmin} {
		if acl.Perms&perm != 0 {
			s += permCharMap[perm]
		} else {
			s += "-"
		}
	}
	return s
}

func cmdChmod(subFlags *flag.FlagSet, args []string) error {
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

	resolved, err := zk.ResolveWildcards(zconn, subFlags.Args()[1:])
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
		aclv, _, err := zconn.ACL(zkPath)
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
		err = zconn.SetACL(zkPath, aclv, -1)
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

func cmdCp(subFlags *flag.FlagSet, args []string) error {
	subFlags.Parse(args)
	switch {
	case subFlags.NArg() < 2:
		return fmt.Errorf("cp: need to specify source and destination paths")
	case subFlags.NArg() == 2:
		return fileCp(args[0], args[1])
	default:
		return multiFileCp(args)
	}
}

func getPathData(filePath string) (string, error) {
	if isZkFile(filePath) {
		data, _, err := zconn.Get(filePath)
		return data, err
	}
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

func setPathData(filePath, data string) error {
	if isZkFile(filePath) {
		_, err := zconn.Set(filePath, data, -1)
		if err == zookeeper.ErrNoNode {
			_, err = zk.CreateRecursive(zconn, filePath, data, 0, zookeeper.WorldACL(zookeeper.PermAll))
		}
		return err
	}
	return ioutil.WriteFile(filePath, []byte(data), 0666)
}

func fileCp(srcPath, dstPath string) error {
	dstIsDir := dstPath[len(dstPath)-1] == '/'
	srcPath = fixZkPath(srcPath)
	dstPath = fixZkPath(dstPath)

	if !isZkFile(srcPath) && !isZkFile(dstPath) {
		return fmt.Errorf("cp: neither src nor dst is a /zk file: exitting")
	}

	data, err := getPathData(srcPath)
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
	if err := setPathData(dstPath, data); err != nil {
		return fmt.Errorf("cp: cannot write %v: %v", dstPath, err)
	}
	return nil
}

func multiFileCp(args []string) error {
	dstPath := args[len(args)-1]
	if dstPath[len(dstPath)-1] != '/' {
		// In multifile context, dstPath must be a directory.
		dstPath += "/"
	}

	for _, srcPath := range args[:len(args)-1] {
		if err := fileCp(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

type zkItem struct {
	path string
	data string
	stat *zookeeper.Stat
	err  error
}

// Store a zk tree in a zip archive. This won't be immediately useful to
// zip tools since even "directories" can contain data.
func cmdZip(subFlags *flag.FlagSet, args []string) error {
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
		children, err := zk.ChildrenRecursive(zconn, zkPath)
		if err != nil {
			return fmt.Errorf("zip: error %v", err)
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
			return fmt.Errorf("zip: get failed: %v", err)
		}
		// Skip ephemerals - not sure why you would archive them.
		if stat.EphemeralOwner > 0 {
			continue
		}
		fi := &zip.FileHeader{Name: path, Method: zip.Deflate}
		fi.SetModTime(zk.Time(stat.Mtime))
		f, err := zipWriter.CreateHeader(fi)
		if err != nil {
			return fmt.Errorf("zip: create failed: %v", err)
		}
		_, err = f.Write([]byte(data))
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

func cmdUnzip(subFlags *flag.FlagSet, args []string) error {
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
		_, err = zk.CreateRecursive(zconn, zkPath, string(data), 0, zookeeper.WorldACL(zookeeper.PermAll))
		if err != nil && err != zookeeper.ErrNodeExists {
			return fmt.Errorf("unzip: zk create failed: %v", err)
		}
		_, err = zconn.Set(zkPath, string(data), -1)
		if err != nil {
			return fmt.Errorf("unzip: zk set failed: %v", err)
		}
		rc.Close()
	}
	return nil
}
