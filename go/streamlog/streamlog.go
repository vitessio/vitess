package streamlog

import (
	"expvar"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
)

var droppedMessages = expvar.NewMap("streamlog-dropped-messages")

// A StreamLogger makes messages sent to it available through HTTP.
type StreamLogger struct {
	dataQueue  chan Stringer
	subscribed map[io.Writer]chan bool
	url        string
	mu         sync.Mutex
	// size is used to check if there are any subscriptions. Keep
	// it atomically in sync with the size of subscribed.
	size uint32
	// seq is a guard for modifications of subscribed - increment
	// it atomically whenever you modify it.
	seq uint32
}

type Stringer interface {
	String() string
}

func (logger *StreamLogger) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	<-logger.subscribe(w)
}

func (logger *StreamLogger) subscribe(w io.Writer) chan bool {
	done := make(chan bool)
	logger.mu.Lock()
	defer logger.mu.Unlock()

	logger.subscribed[w] = done
	atomic.AddUint32(&logger.seq, 1)
	atomic.StoreUint32(&logger.size, uint32(len(logger.subscribed)))
	return done
}

// New returns a new StreamLogger with a buffer that can contain size
// messages. Any messages sent to it will be available at url.
func New(url string, size int) *StreamLogger {
	logger := &StreamLogger{
		dataQueue:  make(chan Stringer, size),
		subscribed: make(map[io.Writer]chan bool),
		url:        url,
	}
	go logger.stream()
	http.Handle(url, logger)
	return logger
}

// stream sends messages sent to logger to all of its subscribed
// writers. This method should be called in a goroutine.
func (logger *StreamLogger) stream() {
	seq := uint32(0)
	var subscribed map[io.Writer]chan bool

	for message := range logger.dataQueue {

		if s := atomic.LoadUint32(&(logger.seq)); s != seq {
			logger.mu.Lock()
			subscribed = make(map[io.Writer]chan bool, len(logger.subscribed))
			for subscription, done := range logger.subscribed {
				subscribed[subscription] = done
			}
			seq = atomic.LoadUint32(&(logger.seq))
			logger.mu.Unlock()
		}

		if len(subscribed) == 0 {
			continue
		}

		messageString := message.String() + "\n"
		for subscription, done := range subscribed {
			if _, err := io.WriteString(subscription, messageString); err != nil {
				done <- true

				logger.mu.Lock()
				delete(logger.subscribed, subscription)
				atomic.AddUint32(&logger.seq, 1)
				atomic.StoreUint32(&logger.size, uint32(len(logger.subscribed)))
				logger.mu.Unlock()
			} else {
				subscription.(http.Flusher).Flush()
			}
		}
	}
}

// Send sends message to all the writers subscribed to logger. Calling
// Send does not block.
func (logger *StreamLogger) Send(message Stringer) {
	if atomic.LoadUint32(&logger.size) == 0 {
		// There are no subscribers, do nothing.
		return
	}
	select {
	case logger.dataQueue <- message:
	default:
		droppedMessages.Add(logger.url, 1)
	}
}
