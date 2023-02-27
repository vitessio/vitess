# Long Running Tasks

Currently, in Vitess, long running tasks are used in a couple places:

* `vtworker` runs tasks that can take a long time, as they can deal with a lot
  of data.
  
* `vttablet` runs backups and restores that can also take a long time.

In both cases, we have a streaming RPC API that starts the job, and streams some
status back while it is running. If the streaming RPC is interrupted, the
behavior is different: `vtworker` and `vttablet backup` will interrupt their
jobs, whereas `vttablet restore` will try to keep going if it is past a point of
no return for restores.

This document proposes a different, but common design for both use cases.

## RPC Model

We introduce a model with three different RPCs:

* Start the job will be a synchronous non-streaming RPC, and just starts the
  job. It returns an identifier for the job.

* Getting the job status is a streaming RPC. It takes the job identifier, and
  streams the current status back. This job status can contain log entries
  (regular messages) or progress (percentage, N/M completion, ...).

* Canceling a running job is its own synchronous non-streaming RPC, and takes
  the job identifier.

For simplicity, let's try to make these the same API for 'vtworker' and
'vttablet'. Usually, a single destination can only run a single job, but let's
not assume that in the API. If a destination process cannot run a job, it should
return the usual `RESOURCE_EXHAUSTED` canonical error code.

These RPCs should be grouped in a new API service. Let's describe it as usual in
`jobdata.proto` and `jobservice.proto`. The current `vtworkerdata.proto` and
`vtworkerservice.proto` will eventually be removed and replaced by the new
service.

Let's use the usual `repeated string args` to describe the job. `vtworker`
already uses that.

So the proposed proto definitions:

``` proto

# in jobdata.proto

message StartRequest {
  repeated string args = 1;
}

message StartResponse {
  string uid = 1;
}

message StatusRequest {
  string uid = 1;
}

// Progress describes the current progress of the task.
// Note the fields here match the Progress and ProgressMessage from the Node
// display of workflows.
message Progress {
  // percentage can be 0-100 if known, or -1 if unknown.
  int8 percentage = 1;

  // message can be empty if percentage is set.
  string message = 2;
}

// FinalStatus describes the end result of a job.
message FinalStatus {
  // error is empty if the job was successful.
  string error = 1;
}

// StatusResponse can have any of its fields set.
message StatusResponse {
  // event is optional, used for logging.
  logutil.Event event = 1;

  // progress is optional, used to indicate progress.
  Progress progress = 2;
  
  // If final_status is set, this is the last StatusResponse for this job,
  // it is terminated.
  FinalStatus final_status = 3;
}

message CancelRequest {
  string uid = 1;
}

message CancelResponse {
}

# in jobdata.service

service Job {
  rpc Start (StartRequest) returns (StartResponse) {};
  
  rpc Status (StatusRequest) returns (stream StatusResponse) {};
  
  rpc Cancel (CancelRequest) returns (CancelResponse) {};
}
```

## Integration with Current Components

### vtworker

This design is very simple to implement within vtworker. At first, we don't need
to link the progress in, just the logging part.

vtworker will only support running one job like this at a time. 

### vttablet

This is also somewhat easy to implement within vttablet. Only `Backup` and
`Restore` will be changed to use this.

vttablet will only support running one job like this at a time. It will also
take the ActionLock, so no other tablet actions can run at the same time (as we
do now).

### vtctld Workflows Integration

The link here is also very straightforward:

* When successfully starting a remote job, the address of the remote worker and
  the UID of the job can be checkpointed.
  
* After that, the workflow can just connect and update its status and logs when
  receiving an update.

* If the workflow is aborted and reloaded somewhere else (vtctld restart), it
  can reconnect to the running job easily.
  
* Canceling the job is also easy, just call the RPC.

### Comments

Both vtworker and vttablet could remember the last N jobs they ran, and their
status. So when a workflow tries to reconnect to a finished job, they just
stream a single `StatusResponse` with a `final_status` field.
