package os

import (
	"fmt"
	"testing"
)

func TestCommandRun(t *testing.T) {
	cmdErr := CommandRun("echo \"VAR1=$VAR1 VAR2=$VAR2\" && exit 11", []string{"VAR1=a", "VAR2=b"})
	if cmdErr == nil {
		t.Error("Expected CommandRun to fail, but no error returned")
	}

	expectedMsg := "(exit status 11) VAR1=a VAR2=b\n"
	if cmdErr.Error() != expectedMsg {
		t.Errorf(fmt.Sprintf("Expected CommandRun to return an Error '%s' but got '%s'", expectedMsg, cmdErr.Error()))
	}
}
