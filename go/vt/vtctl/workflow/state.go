package workflow

// Type is the type of a workflow.
type Type string

// Workflow types.
const (
	TypeReshard    Type = "Reshard"
	TypeMoveTables Type = "MoveTables"
)

// State represents the state of a workflow.
type State struct {
	Workflow       string
	SourceKeyspace string
	TargetKeyspace string
	WorkflowType   Type

	ReplicaCellsSwitched    []string
	ReplicaCellsNotSwitched []string

	RdonlyCellsSwitched    []string
	RdonlyCellsNotSwitched []string

	WritesSwitched bool
}
