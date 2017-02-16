package resharding

import "github.com/youtube/vitess/go/vt/workflow"

// RetryController stores the data for controlling the retry action.
type RetryController struct {
	node *workflow.Node
	// retryChannel is used to trigger the retrying of task
	// when pressing the button.
	retryChannel chan struct{}
}

// CreateRetryController create a RetryController for a specific node and
// enable the retry action on the node.
func CreateRetryController(node *workflow.Node, actionListener workflow.ActionListener) *RetryController {
	retryAction := &workflow.Action{
		Name:  "Retry",
		State: workflow.ActionStateEnabled,
		Style: workflow.ActionStyleWaiting,
	}
	node.Actions = []*workflow.Action{retryAction}
	node.Listener = actionListener
	return &RetryController{
		node:         node,
		retryChannel: make(chan struct{}),
	}
}

// triggerRetry closes the retryChannel and empties the Actions list
// in the UI Node. This disables the retry action.
func (c *RetryController) triggerRetry() {
	if len(c.node.Actions) != 0 {
		c.node.Actions = []*workflow.Action{}
		close(c.retryChannel)
	}
	c.node.BroadcastChanges(false /* updateChildren */)
}
