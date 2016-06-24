package schema

import (
	"fmt"
	"testing"

	"github.com/youtube/vitess/go/cistring"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestTableColumnString(t *testing.T) {
	c := &TableColumn{Name: cistring.New("my_column"), Type: querypb.Type_INT8}
	expected := "{Name: 'my_column', Type: INT8}"
	actual := fmt.Sprintf("%v", c)
	if actual != expected {
		t.Errorf("want: %v, got: %v", expected, actual)
	}
}
