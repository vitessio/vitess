package zkocc

import (
	"errors"
	"sync"

	"code.google.com/p/vitess/go/zk"
	"launchpad.net/gozk/zookeeper"
)

// zkocc
//
// Cache open zk connections and allow cheap read requests.
// Cache data coming back from zk.
//  * Use node notifications for data invalidation and freshness.
//  * Force invalidation after some amount of time with some amount of skew.
//  * Insulate this process from periodic zk cell failures.
//    * On reconnect, re-read all paths.
//    * Report ZkNode as stale when things are disconnected?
type ZkPath struct {
	Path string
}

type ZkPathV struct {
	Paths []string
}

type ZkNode struct {
	Path     string
	Stat     *zookeeper.Stat
	Data     string
	Children []string
}

type ZkNodeV struct {
	Nodes []*ZkNode
}

type ZkReader struct {
	zconn zk.Conn
}

var (
	ErrPartialRead = errors.New("zkocc: partial read")
)

func NewZkReader(conn zk.Conn) *ZkReader {
	return &ZkReader{conn}
}

func (zkr *ZkReader) Get(req *ZkPath, reply *ZkNode) (err error) {
	reply.Path = req.Path
	reply.Data, reply.Stat, err = zkr.zconn.Get(req.Path)
	return
}

func (zkr *ZkReader) GetV(req *ZkPathV, reply *ZkNodeV) error {
	wg := sync.WaitGroup{}
	mu := sync.Mutex{}

	reply.Nodes = make([]*ZkNode, 0, len(req.Paths))
	errors := make([]error, 0, len(req.Paths))
	for _, zkPath := range req.Paths {
		zn := &ZkNode{Path: zkPath}
		wg.Add(1)
		go func() {
			data, stat, err := zkr.zconn.Get(zn.Path)
			mu.Lock()
			if err != nil {
				errors = append(errors, err)
			} else {
				zn.Data = data
				zn.Stat = stat
				reply.Nodes = append(reply.Nodes, zn)
			}
			mu.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if len(errors) > 0 {
		return ErrPartialRead
	}
	return nil
}

func (zkr *ZkReader) Children(req *ZkPath, reply *ZkNode) (err error) {
	reply.Path = req.Path
	reply.Children, reply.Stat, err = zkr.zconn.Children(req.Path)
	return
}
