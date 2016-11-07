package workflow

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/timer"
)

const (
	jsonContentType = "application/json; charset=utf-8"

	// dropTimeout is the minimum age for us to drop a webConnection.
	dropTimeout = time.Minute
)

// longPollingConnection holds information about a long polling HTTP client.
type longPollingConnection struct {
	notifications    chan []byte
	nodeManagerIndex int

	// lastPoll is protected by the longPollingManager mu Mutex.
	lastPoll time.Time
}

// longPollingManager holds informations about the virtual channels.
type longPollingManager struct {
	// manager is set at creation time and immutable.
	manager *Manager

	// mu protects the connections map.
	mu sync.Mutex
	// connections maps the node manager channel index to a
	// longPollingConnection objects. The key in the map is equal
	// to the nodeManagerIndex field of the pointed object.
	connections map[int]*longPollingConnection
}

func newLongPollingManager(m *Manager) *longPollingManager {
	lpm := &longPollingManager{
		manager:     m,
		connections: make(map[int]*longPollingConnection),
	}
	t := timer.NewTimer(dropTimeout)
	t.Start(func() {
		lpm.checkStaleConnections()
	})
	return lpm
}

func (lpm *longPollingManager) create(servedURL *url.URL) ([]byte, int, error) {
	tree, notifications, i, err := lpm.manager.getAndWatchFullTree(servedURL)
	if err != nil {
		return nil, 0, fmt.Errorf("getAndWatchFullTree failed: %v", err)
	}

	if notifications != nil {
		lpm.mu.Lock()
		defer lpm.mu.Unlock()
		lpm.connections[i] = &longPollingConnection{
			notifications:    notifications,
			nodeManagerIndex: i,
			lastPoll:         time.Now(),
		}
	}

	return tree, i, err
}

func (lpm *longPollingManager) poll(i int) ([]byte, error) {
	lpm.mu.Lock()
	wc, ok := lpm.connections[i]
	if !ok {
		lpm.mu.Unlock()
		return nil, fmt.Errorf("unknown id: %v", i)
	}
	wc.lastPoll = time.Now()
	lpm.mu.Unlock()

	select {
	case update, ok := <-wc.notifications:
		if !ok {
			lpm.mu.Lock()
			delete(lpm.connections, i)
			lpm.mu.Unlock()
			return nil, fmt.Errorf("notifications channel closed, probably because client didn't read messages fast enough")
		}
		return update, nil
	case <-time.After(20 * time.Second):
		return []byte("{}"), nil
	}
}

func (lpm *longPollingManager) remove(i int) {
	lpm.mu.Lock()
	defer lpm.mu.Unlock()
	if _, ok := lpm.connections[i]; !ok {
		return
	}
	delete(lpm.connections, i)
	lpm.manager.NodeManager().CloseWatcher(i)
}

func (lpm *longPollingManager) checkStaleConnections() {
	now := time.Now()
	lpm.mu.Lock()
	defer lpm.mu.Unlock()
	for i, wc := range lpm.connections {
		if now.Sub(wc.lastPoll) > dropTimeout {
			log.Infof("dropping stale webapi workflow connection %v", i)
			delete(lpm.connections, i)
		}
	}
}

// getID extracts the id from the url. The URL looks like:
// /api/workflow/poll/123 and the provided base is something like:
// /api/workflow/poll/  so we just remove the base and convert the integer.
func getID(url, base string) (int, error) {
	// Strip API prefix.
	if !strings.HasPrefix(url, base) {
		return 0, fmt.Errorf("bad url prefix for %v", url)
	}
	idStr := url[len(base):]

	i, err := strconv.Atoi(idStr)
	if err != nil {
		return 0, fmt.Errorf("bad listener id %v", idStr)
	}
	return i, nil
}

func httpErrorf(w http.ResponseWriter, r *http.Request, format string, args ...interface{}) {
	errMsg := fmt.Sprintf(format, args...)
	log.Errorf("HTTP error on %v: %v, request: %#v", r.URL.Path, errMsg, r)
	http.Error(w, errMsg, http.StatusInternalServerError)
}

func handleAPI(pattern string, handlerFunc func(w http.ResponseWriter, r *http.Request) error) {
	http.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if x := recover(); x != nil {
				httpErrorf(w, r, "uncaught panic: %v", x)
			}
		}()

		if err := acl.CheckAccessHTTP(r, acl.ADMIN); err != nil {
			httpErrorf(w, r, "WorkflowManager acl.CheckAccessHTTP failed: %v", err)
			return
		}

		if err := handlerFunc(w, r); err != nil {
			httpErrorf(w, r, "%v", err)
		}
	})
}

// HandleHTTPLongPolling registers the streaming-over-HTTP APIs.
func (m *Manager) HandleHTTPLongPolling(pattern string) {
	log.Infof("workflow Manager listening to web traffic at %v/{create,poll,delete}", pattern)
	lpm := newLongPollingManager(m)

	handleAPI(pattern+"/create", func(w http.ResponseWriter, r *http.Request) error {
		result, i, err := lpm.create(r.URL)
		if err != nil {
			return fmt.Errorf("longPollingManager.create failed: %v", err)
		}

		w.Header().Set("Content-Type", jsonContentType)
		w.Header().Set("Content-Length", fmt.Sprintf("%v", len(result)))
		if _, err := w.Write(result); err != nil {
			lpm.remove(i)
		}
		return nil
	})

	handleAPI(pattern+"/poll/", func(w http.ResponseWriter, r *http.Request) error {
		i, err := getID(r.URL.Path, pattern+"/poll/")
		if err != nil {
			return err
		}

		result, err := lpm.poll(i)
		if err != nil {
			return fmt.Errorf("longPollingManager.poll failed: %v", err)
		}

		w.Header().Set("Content-Type", jsonContentType)
		w.Header().Set("Content-Length", fmt.Sprintf("%v", len(result)))
		if _, err := w.Write(result); err != nil {
			lpm.remove(i)
		}
		return nil
	})

	handleAPI(pattern+"/remove/", func(w http.ResponseWriter, r *http.Request) error {
		i, err := getID(r.URL.Path, pattern+"/remove/")
		if err != nil {
			return err
		}

		lpm.remove(i)
		http.Error(w, "", http.StatusOK)
		return nil
	})

	handleAPI(pattern+"/action/", func(w http.ResponseWriter, r *http.Request) error {
		_, err := getID(r.URL.Path, pattern+"/action/")
		if err != nil {
			return err
		}

		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return err
		}

		ap := &ActionParameters{}
		if err := json.Unmarshal(data, ap); err != nil {
			return err
		}

		ctx := context.TODO()
		if err := m.NodeManager().Action(ctx, ap); err != nil {
			return fmt.Errorf("Action failed: %v", err)
		}
		http.Error(w, "", http.StatusOK)
		return nil
	})
}
