package rpcplus

import (
	"errors"
	"log"
	"net"
	"strings"
	"testing"
	"time"
)

const (
	httpPath = "/srpc"
)

type StreamingArgs struct {
	A     int
	Count int
	// next two values have to be between 0 and Count-2 to trigger anything
	ErrorAt   int // will trigger an error at the given spot,
	BadTypeAt int // will send the wrong type in sendReply
}

type StreamingReply struct {
	C     int
	Index int
}

type StreamingArith int

func (t *StreamingArith) Thrive(args StreamingArgs, sendReply func(reply interface{}) error) error {

	for i := 0; i < args.Count; i++ {
		if i == args.ErrorAt {
			return errors.New("Triggered error in middle")
		}
		if i == args.BadTypeAt {
			// send args instead of response
			sr := new(StreamingArgs)
			err := sendReply(sr)
			if err != nil {
				return err
			}
		}
		// log.Println("  Sending sample", i)
		sr := &StreamingReply{C: args.A, Index: i}
		err := sendReply(sr)
		if err != nil {
			return err
		}
	}

	return nil
}

// make a server, a cient, and connect them
func makeLink(t *testing.T) (client *Client) {
	// start a server
	server := NewServer()
	err := server.Register(new(StreamingArith))
	if err != nil {
		t.Fatal("Register failed", err)
	}

	// listen and handle queries
	var l net.Listener
	l, serverAddr = listenTCP()
	log.Println("Test RPC server listening on", serverAddr)
	go server.Accept(l)

	// dial the client
	client, err = Dial("tcp", serverAddr)
	if err != nil {
		t.Fatal("dialing", err)
	}

	return
}

// this is a specific function so we can call it to check the link
// is still active and well
func callOnceAndCheck(t *testing.T, client *Client) {

	args := &StreamingArgs{3, 5, -1, -1}
	rowChan := make(chan *StreamingReply, 10)
	c := client.StreamGo("StreamingArith.Thrive", args, rowChan)

	count := 0
	for row := range rowChan {
		if row.Index != count {
			t.Fatal("unexpected value:", row.Index)
		}
		count += 1

		// log.Println("Values: ", row.C, row.Index)
	}

	if c.Error != nil {
		t.Fatal("unexpected error:", c.Error.Error())
	}

	if count != 5 {
		t.Fatal("Didn't receive the right number of packets back:", count)
	}
}

func TestStreamingRpc(t *testing.T) {

	client := makeLink(t)

	// Nonexistent method
	args := &StreamingArgs{7, 10, -1, -1}
	reply := new(StreamingReply)
	err := client.Call("StreamingArith.BadOperation", args, reply)
	// expect an error
	if err == nil {
		t.Error("BadOperation: expected error")
	} else if !strings.HasPrefix(err.Error(), "rpc: can't find method ") {
		t.Errorf("BadOperation: expected can't find method error; got %q", err)
	}

	// call that works
	callOnceAndCheck(t, client)

	// call that may block forever (but won't!)
	args = &StreamingArgs{3, 100, -1, -1} // 100 is greater than the next 10
	rowChan := make(chan *StreamingReply, 10)
	client.StreamGo("StreamingArith.Thrive", args, rowChan)
	// read one guy, sleep a bit to make sure everything went
	// through, then close
	_, ok := <-rowChan
	if !ok {
		t.Fatal("unexpected closed channel")
	}
	time.Sleep(time.Second)

	// log.Println("Closing")
	client.Close()
	for _ = range rowChan {
	}
	// log.Println("Closed")

	// the sleep here is intended to show the log at the end of the input()
	// go routine, to make sure it existed. Not sure how to test it
	// programmatically (short of having a counter in the library
	// on how many input() threads we have?)
	time.Sleep(time.Second)
}

func TestInterruptedCallByServer(t *testing.T) {

	// make our client
	client := makeLink(t)

	args := &StreamingArgs{3, 100, 30, -1} // 30 elements back, then error
	rowChan := make(chan *StreamingReply, 10)
	c := client.StreamGo("StreamingArith.Thrive", args, rowChan)

	// check we get the error at the 30th call exactly
	count := 0
	for row := range rowChan {
		if row.Index != count {
			t.Fatal("unexpected value:", row.Index)
		}
		count += 1
	}
	if count != 30 {
		t.Fatal("received error before the right time:", count)
	}
	if c.Error.Error() != "Triggered error in middle" {
		t.Fatal("received wrong error message:", c.Error)
	}

	// make sure the wire is still in good shape
	callOnceAndCheck(t, client)

	// then check a call that doesn't send anything, but errors out first
	args = &StreamingArgs{3, 100, 0, -1}
	rowChan = make(chan *StreamingReply, 10)
	c = client.StreamGo("StreamingArith.Thrive", args, rowChan)
	_, ok := <-rowChan
	if ok {
		t.Fatal("expected closed channel")
	}
	if c.Error.Error() != "Triggered error in middle" {
		t.Fatal("received wrong error message:", c.Error)
	}

	// make sure the wire is still in good shape
	callOnceAndCheck(t, client)
}

func TestBadTypeByServer(t *testing.T) {

	// make our client
	client := makeLink(t)

	args := &StreamingArgs{3, 100, -1, 30} // 30 elements back, then bad
	rowChan := make(chan *StreamingReply, 10)
	c := client.StreamGo("StreamingArith.Thrive", args, rowChan)

	// check we get the error at the 30th call exactly
	count := 0
	for row := range rowChan {
		if row.Index != count {
			t.Fatal("unexpected value:", row.Index)
		}
		count += 1
	}
	if count != 30 {
		t.Fatal("received error before the right time:", count)
	}
	if c.Error.Error() != "rpc: passing wrong type to sendReply" {
		t.Fatal("received wrong error message:", c.Error)
	}

	// make sure the wire is still in good shape
	callOnceAndCheck(t, client)
}
