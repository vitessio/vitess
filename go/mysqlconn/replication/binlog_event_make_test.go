package replication

import (
	"reflect"
	"testing"
)

func TestFormatDescriptionEvent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	event := NewFormatDescriptionEvent(f, s)
	if !event.IsValid() {
		t.Fatalf("IsValid() returned false")
	}
	if !event.IsFormatDescription() {
		t.Fatalf("IsFormatDescription returned false")
	}
	gotF, err := event.Format()
	if err != nil {
		t.Fatalf("Format failed: %v", err)
	}
	if !reflect.DeepEqual(gotF, f) {
		t.Fatalf("Parsed BinlogFormat doesn't match, got:\n%v\nexpected:\n%v", gotF, f)
	}
}

func TestInvalidEvents(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	// InvalidEvent
	event := NewInvalidEvent()
	if event.IsValid() {
		t.Fatalf("NewInvalidEvent().IsValid() is true")
	}

	// InvalidFormatDescriptionEvent
	event = NewInvalidFormatDescriptionEvent(f, s)
	if !event.IsValid() {
		t.Fatalf("NewInvalidFormatDescriptionEvent().IsValid() is false")
	}
	if !event.IsFormatDescription() {
		t.Fatalf("NewInvalidFormatDescriptionEvent().IsFormatDescription() is false")
	}
	if _, err := event.Format(); err == nil {
		t.Fatalf("NewInvalidFormatDescriptionEvent().Format() returned err=nil")
	}

	// InvalidQueryEvent
	event = NewInvalidQueryEvent(f, s)
	if !event.IsValid() {
		t.Fatalf("NewInvalidQueryEvent().IsValid() is false")
	}
	if !event.IsQuery() {
		t.Fatalf("NewInvalidQueryEvent().IsQuery() is false")
	}
	if _, err := event.Query(f); err == nil {
		t.Fatalf("NewInvalidQueryEvent().Query() returned err=nil")
	}
}
