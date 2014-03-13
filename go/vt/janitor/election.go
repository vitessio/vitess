package janitor

// TODO(szopa): Implement master election using ZooKeeper.

func (scheduler *Scheduler) IsMaster() bool {
	return true
}

func (scheduler *Scheduler) RunElection() {
}

func (scheduler *Scheduler) CurrentMasterID() string {
	return "master election not implemented"
}
