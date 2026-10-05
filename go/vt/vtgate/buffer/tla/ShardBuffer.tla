---------------------------- MODULE ShardBuffer ----------------------------
(***************************************************************************)
(* TLA+ model of go/vt/vtgate/buffer (shard_buffer.go, timeout_thread.go,  *)
(* buffer.go) for a single shard.                                          *)
(*                                                                         *)
(* Granularity: every critical section that runs entirely under sb.mu is   *)
(* one atomic step (none of them block). Lock-free reads (the atomic       *)
(* state fast path, channel selects, WaitGroup waits) are separate steps,  *)
(* so TLC explores every interleaving between them.                        *)
(*                                                                         *)
(* Goroutines modelled:                                                    *)
(*   - request goroutines (tabletgateway.withRetry -> WaitForFailoverEnd)  *)
(*   - the timeout thread                                                  *)
(*   - the drain goroutine (with DrainConcurrency workers)                 *)
(*   - async waitForRequestFinish goroutines (sb.wg)                       *)
(*   - keyspace events (HandleKeyspaceEvent -> stopBufferingLocked)        *)
(*   - Buffer.Shutdown                                                     *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Req,                  \* request goroutines
    MaxAttempts,          \* WaitForFailoverEnd calls per request goroutine
    Size,                 \* --buffer-size (bufferSizeSema capacity)
    DrainConcurrency,     \* --buffer-drain-concurrency
    FairMaxDuration,      \* TRUE: max-failover-duration timer eventually fires
    \* The Bug* constants re-introduce past bugs to show that the model catches
    \* them. They must all be FALSE to model the current code.
    BugLostWakeup,        \* TRUE: timeout thread reads queueNotEmpty after oldestEntry()
    BugNoStoppedRecheck,  \* TRUE: no Buffer.stopped re-check under sb.mu (#19954)
    BugRemoveNoRelease    \* TRUE: remove() does not release the buffer slot

Att     == 0..(MaxAttempts - 1)
Entries == Req \X Att
NoEntry == <<"none", 0>>

VARIABLES
    state,        \* sb.state: "idle" | "buffering" | "draining"
    stopped,      \* Buffer.stopped
    queue,        \* sb.queue
    sema,         \* free slots in bufferSizeSema
    done,         \* entry.done closed
    bufCtxDone,   \* entry.bufferCtx done (bufferCancel called or parent ctx done)
    ctxCanceled,  \* parent request ctx canceled
    expired,      \* entry exceeded its buffering window
    pc, att, fd,  \* request goroutine program counter, attempt, failoverDetected
    waiters,      \* async waitForRequestFinish goroutines: set of [e, rel]
    drainPc, drainQ, drainInflight,
    ttPc,         \* timeout thread program counter ("none" = not running)
    ttEntry,      \* entry the timeout thread is waiting on
    ttStop,       \* tt.stopChan closed
    maxFired,     \* tt.maxDuration timer fired
    snapCurrent,  \* the queueNotEmpty channel the timeout thread holds is still open
    shPc,         \* Buffer.Shutdown progress
    errs          \* runtime failures observed (panics, misuse)

vars == <<state, stopped, queue, sema, done, bufCtxDone, ctxCanceled, expired,
          pc, att, fd, waiters, drainPc, drainQ, drainInflight,
          ttPc, ttEntry, ttStop, maxFired, snapCurrent, shPc, errs>>

reqVars   == <<pc, att, fd>>
entryVars == <<done, bufCtxDone, ctxCanceled, expired>>
ttVars    == <<ttPc, ttEntry, ttStop, maxFired, snapCurrent>>
drainVars == <<drainPc, drainQ, drainInflight>>

Cur(r) == <<r, att[r]>>

InQueue(e) == \E i \in 1..Len(queue) : queue[i] = e

RemoveFrom(s, e) == SelectSeq(s, LAMBDA x : x # e)

\* sb.wg counter: async waiters + the drain goroutine.
WG == Cardinality(waiters) + (IF drainPc # "none" THEN 1 ELSE 0)

\* sync.WaitGroup: Add with positive delta while counter is zero and Wait is
\* in progress is a misuse (may panic / Wait may miss the goroutine).
WGAddErr(adding) ==
    IF adding /\ shPc = "waiting" /\ WG = 0
    THEN {"sync.WaitGroup: Add called concurrently with Wait"} ELSE {}

CloseErr(e) == IF done[e] THEN {"close of closed channel"} ELSE {}

\* bufferSizeSema.Release(1) panics when releasing more than held.
ReleaseSema ==
    IF sema >= Size
    THEN /\ errs' = errs \cup {"semaphore: released more than held"}
         /\ UNCHANGED sema
    ELSE /\ sema' = sema + 1
         /\ UNCHANGED errs

\* Request finished this WaitForFailoverEnd call; the goroutine may call again.
EndAttempt(r) ==
    /\ pc'  = [pc EXCEPT ![r] = IF att[r] + 1 < MaxAttempts THEN "start" ELSE "fin"]
    /\ att' = [att EXCEPT ![r] = @ + 1]
    /\ fd'  = [fd EXCEPT ![r] = FALSE]

-----------------------------------------------------------------------------
Init ==
    /\ state = "idle"
    /\ stopped = FALSE
    /\ queue = <<>>
    /\ sema = Size
    /\ done = [e \in Entries |-> FALSE]
    /\ bufCtxDone = [e \in Entries |-> FALSE]
    /\ ctxCanceled = [e \in Entries |-> FALSE]
    /\ expired = [e \in Entries |-> FALSE]
    /\ pc = [r \in Req |-> "start"]
    /\ att = [r \in Req |-> 0]
    /\ fd = [r \in Req |-> FALSE]
    /\ waiters = {}
    /\ drainPc = "none"
    /\ drainQ = <<>>
    /\ drainInflight = {}
    /\ ttPc = "none"
    /\ ttEntry = NoEntry
    /\ ttStop = FALSE
    /\ maxFired = FALSE
    /\ snapCurrent = FALSE
    /\ shPc = "none"
    /\ errs = {}

-----------------------------------------------------------------------------
(* shardBuffer.shouldBuffer / shouldBufferLocked *)
ShouldBuffer(f) == state = "buffering" \/ (state = "idle" /\ f)

(* stopBufferingLocked: called with sb.mu held. *)
StopBufferingLocked ==
    IF state = "buffering"
    THEN /\ state' = "draining"
         /\ drainQ' = queue
         /\ queue' = <<>>
         /\ drainPc' = "stopTT"
         /\ errs' = errs \cup WGAddErr(TRUE)
                         \cup (IF drainPc # "none" THEN {"second drain started"} ELSE {})
         /\ UNCHANGED drainInflight
    ELSE UNCHANGED <<state, drainQ, queue, drainPc, drainInflight, errs>>

-----------------------------------------------------------------------------
(* Request goroutine: Buffer.WaitForFailoverEnd *)

\* getOrCreateBuffer() stopped check + lock-free fast path (shouldBuffer).
ReqStart(r) ==
    /\ pc[r] = "start"
    /\ \E f \in BOOLEAN :
        /\ fd' = [fd EXCEPT ![r] = f]
        /\ IF stopped \/ ~ShouldBuffer(f)
           THEN /\ pc' = [pc EXCEPT ![r] = "end"]
           ELSE /\ pc' = [pc EXCEPT ![r] = "slow"]
    /\ UNCHANGED <<att, state, stopped, queue, sema, entryVars, waiters,
                   drainVars, ttVars, shPc, errs>>

ReqEnd(r) ==
    /\ pc[r] = "end"
    /\ EndAttempt(r)
    /\ UNCHANGED <<state, stopped, queue, sema, entryVars, waiters,
                   drainVars, ttVars, shPc, errs>>

\* Slow path: everything between sb.mu.Lock() and sb.mu.Unlock() in
\* waitForFailoverEnd, including startBufferingLocked and bufferRequestLocked.
ReqSlow(r) ==
    LET e    == Cur(r)
        bail == (stopped /\ ~BugNoStoppedRecheck) \/ ~ShouldBuffer(fd[r])
    IN
    /\ pc[r] = "slow"
    /\ \/ \* Not buffering (stopped, state changed, last failover too recent,
          \* or KeyspaceEventWatcher refused MarkShardNotServing).
          /\ bail \/ state = "idle"
          /\ pc' = [pc EXCEPT ![r] = "end"]
          /\ UNCHANGED <<state, queue, sema, done, waiters, ttVars, drainVars, errs>>
       \/ /\ ~bail
          /\ LET starting == state = "idle"
                 q0       == IF starting THEN <<>> ELSE queue
                 full     == sema = 0
                 h        == Head(q0)
                 q1       == IF full THEN Tail(q0) ELSE q0
                 startErr == IF starting /\ ttPc # "none"
                             THEN {"timeout thread still running at start"} ELSE {}
             IN
             /\ state' = "buffering"
             /\ IF starting
                THEN /\ ttPc' = "loop"
                     /\ ttStop' = FALSE
                     /\ maxFired' = FALSE
                     /\ ttEntry' = NoEntry
                ELSE UNCHANGED <<ttPc, ttStop, maxFired, ttEntry>>
             /\ IF full /\ q0 = <<>>
                THEN \* bufferFullError
                     /\ queue' = q0
                     /\ pc' = [pc EXCEPT ![r] = "end"]
                     /\ snapCurrent' = IF starting THEN FALSE ELSE snapCurrent
                     /\ errs' = errs \cup startErr
                     /\ UNCHANGED <<sema, done, waiters>>
                ELSE \* Evict the oldest entry if full, then enqueue.
                     /\ queue' = Append(q1, e)
                     /\ sema' = IF full THEN sema ELSE sema - 1
                     /\ done' = IF full THEN [done EXCEPT ![h] = TRUE] ELSE done
                     /\ waiters' = IF full THEN waiters \cup {[e |-> h, rel |-> FALSE]}
                                           ELSE waiters
                     /\ errs' = errs \cup startErr
                                     \cup (IF full THEN CloseErr(h) \cup WGAddErr(TRUE) ELSE {})
                     \* len(queue) == 1 -> notifyQueueNotEmpty()
                     /\ snapCurrent' = IF starting \/ Len(q1) = 0 THEN FALSE ELSE snapCurrent
                     /\ pc' = [pc EXCEPT ![r] = "wait"]
             /\ UNCHANGED drainVars
    /\ UNCHANGED <<att, fd, stopped, bufCtxDone, ctxCanceled, expired, shPc>>

\* shardBuffer.wait(): select { case <-ctx.Done(): case <-e.done: }
ReqWait(r) ==
    LET e == Cur(r) IN
    /\ pc[r] = "wait"
    /\ \/ /\ done[e]
          /\ pc' = [pc EXCEPT ![r] = "retry"]
       \/ /\ ctxCanceled[e]
          /\ pc' = [pc EXCEPT ![r] = "remove"]
    /\ UNCHANGED <<att, fd, state, stopped, queue, sema, entryVars, waiters,
                   drainVars, ttVars, shPc, errs>>

\* shardBuffer.remove()
ReqRemove(r) ==
    LET e == Cur(r) IN
    /\ pc[r] = "remove"
    /\ IF InQueue(e)
       THEN /\ queue' = RemoveFrom(queue, e)
            /\ bufCtxDone' = [bufCtxDone EXCEPT ![e] = TRUE]
            /\ done' = [done EXCEPT ![e] = TRUE]
            /\ waiters' = waiters \cup {[e |-> e, rel |-> ~BugRemoveNoRelease]}
            /\ errs' = errs \cup CloseErr(e) \cup WGAddErr(TRUE)
       ELSE UNCHANGED <<queue, bufCtxDone, done, waiters, errs>>
    /\ EndAttempt(r)
    /\ UNCHANGED <<state, stopped, sema, ctxCanceled, expired,
                   drainVars, ttVars, shPc>>

\* The request was unblocked and retries; withRetry eventually calls retryDone
\* (= bufferCancel) via defer.
ReqRetry(r) ==
    LET e == Cur(r) IN
    /\ pc[r] = "retry"
    /\ bufCtxDone' = [bufCtxDone EXCEPT ![e] = TRUE]
    /\ EndAttempt(r)
    /\ UNCHANGED <<state, stopped, queue, sema, done, ctxCanceled, expired,
                   waiters, drainVars, ttVars, shPc, errs>>

\* The client's context is canceled (deadline, disconnect). bufferCtx is a child.
ReqCancel(r) ==
    LET e == Cur(r) IN
    /\ pc[r] \in {"wait", "retry"}
    /\ ~ctxCanceled[e]
    /\ ctxCanceled' = [ctxCanceled EXCEPT ![e] = TRUE]
    /\ bufCtxDone' = [bufCtxDone EXCEPT ![e] = TRUE]
    /\ UNCHANGED <<reqVars, state, stopped, queue, sema, done, expired, waiters,
                   drainVars, ttVars, shPc, errs>>

-----------------------------------------------------------------------------
(* Time passes: the head entry's deadline is reached. *)
Expire ==
    /\ queue # <<>>
    /\ ~expired[Head(queue)]
    /\ expired' = [expired EXCEPT ![Head(queue)] = TRUE]
    /\ UNCHANGED <<reqVars, state, stopped, queue, sema, done, bufCtxDone,
                   ctxCanceled, waiters, drainVars, ttVars, shPc, errs>>

(* waitForRequestFinish(async=true) goroutine *)
WaiterFinish(w) ==
    /\ bufCtxDone[w.e]
    /\ waiters' = waiters \ {w}
    /\ IF w.rel THEN ReleaseSema ELSE UNCHANGED <<sema, errs>>
    /\ UNCHANGED <<reqVars, state, stopped, queue, entryVars, drainVars, ttVars, shPc>>

-----------------------------------------------------------------------------
(* Timeout thread: timeoutThread.run() *)

\* Top of the for loop.
\*   Current code:  queueNotEmpty := tt.currentQueueNotEmpty(), then
\*                  e := sb.oldestEntry().
\*   BugLostWakeup: e := sb.oldestEntry() first and only if e == nil read
\*                  tt.queueNotEmpty, after sb.mu was released.
TTLoop ==
    /\ ttPc = "loop"
    /\ IF ~BugLostWakeup
       THEN /\ snapCurrent' = TRUE
            /\ ttPc' = "check"
            /\ UNCHANGED ttEntry
       ELSE /\ IF queue # <<>>
               THEN /\ ttEntry' = Head(queue)
                    /\ ttPc' = "waitEntry"
               ELSE /\ ttPc' = "snap"
                    /\ UNCHANGED ttEntry
            /\ UNCHANGED snapCurrent
    /\ UNCHANGED <<reqVars, state, stopped, queue, sema, entryVars, waiters,
                   drainVars, ttStop, maxFired, shPc, errs>>

\* oldestEntry() after reading queueNotEmpty.
TTCheck ==
    /\ ttPc = "check"
    /\ IF queue # <<>>
       THEN /\ ttEntry' = Head(queue)
            /\ ttPc' = "waitEntry"
       ELSE /\ ttPc' = "waitNE"
            /\ UNCHANGED ttEntry
    /\ UNCHANGED <<reqVars, state, stopped, queue, sema, entryVars, waiters,
                   drainVars, ttStop, maxFired, snapCurrent, shPc, errs>>

\* BugLostWakeup only: read tt.queueNotEmpty after oldestEntry() saw an empty queue.
TTSnap ==
    /\ ttPc = "snap"
    /\ snapCurrent' = TRUE
    /\ ttPc' = "waitNE"
    /\ UNCHANGED <<reqVars, state, stopped, queue, sema, entryVars, waiters,
                   drainVars, ttEntry, ttStop, maxFired, shPc, errs>>

\* select in waitForNonEmptyQueue()
TTWaitNE ==
    /\ ttPc = "waitNE"
    /\ \/ maxFired /\ ttPc' = "stopBuf"
       \/ ttStop /\ ttPc' = "none"
       \/ ~snapCurrent /\ ttPc' = "loop"
    /\ UNCHANGED <<reqVars, state, stopped, queue, sema, entryVars, waiters,
                   drainVars, ttEntry, ttStop, maxFired, snapCurrent, shPc, errs>>

\* select in waitForEntry()
TTWaitEntry ==
    /\ ttPc = "waitEntry"
    /\ \/ maxFired /\ ttPc' = "stopBuf" /\ ttEntry' = NoEntry
       \/ ttStop /\ ttPc' = "none" /\ ttEntry' = NoEntry
       \/ done[ttEntry] /\ ttPc' = "loop" /\ ttEntry' = NoEntry
       \/ expired[ttEntry] /\ ttPc' = "evict" /\ UNCHANGED ttEntry
    /\ UNCHANGED <<reqVars, state, stopped, queue, sema, entryVars, waiters,
                   drainVars, ttStop, maxFired, snapCurrent, shPc, errs>>

\* evictOldestEntry()
TTEvict ==
    /\ ttPc = "evict"
    /\ IF queue # <<>> /\ Head(queue) = ttEntry
       THEN /\ queue' = Tail(queue)
            /\ done' = [done EXCEPT ![ttEntry] = TRUE]
            /\ waiters' = waiters \cup {[e |-> ttEntry, rel |-> TRUE]}
            /\ errs' = errs \cup CloseErr(ttEntry) \cup WGAddErr(TRUE)
       ELSE UNCHANGED <<queue, done, waiters, errs>>
    /\ ttPc' = "loop"
    /\ ttEntry' = NoEntry
    /\ UNCHANGED <<reqVars, state, stopped, sema, bufCtxDone, ctxCanceled, expired,
                   drainVars, ttStop, maxFired, snapCurrent, shPc>>

\* stopBufferingDueToMaxDuration(), then the thread returns.
TTStopBuf ==
    /\ ttPc = "stopBuf"
    /\ StopBufferingLocked
    /\ ttPc' = "none"
    /\ UNCHANGED <<reqVars, stopped, sema, entryVars, waiters,
                   ttEntry, ttStop, maxFired, snapCurrent, shPc>>

\* The --buffer-max-failover-duration timer fires.
MaxDurationFires ==
    /\ ttPc # "none"
    /\ ~maxFired
    /\ maxFired' = TRUE
    /\ UNCHANGED <<reqVars, state, stopped, queue, sema, entryVars, waiters,
                   drainVars, ttPc, ttEntry, ttStop, snapCurrent, shPc, errs>>

-----------------------------------------------------------------------------
(* Keyspace event: recordKeyspaceEvent() -> stopBufferingLocked() *)
KeyspaceEvent ==
    /\ state = "buffering"
    /\ StopBufferingLocked
    /\ UNCHANGED <<reqVars, stopped, sema, entryVars, waiters, ttVars, shPc>>

-----------------------------------------------------------------------------
(* drain() goroutine *)

\* sb.timeoutThread.stop(): close(stopChan) ...
DrainStopTT ==
    /\ drainPc = "stopTT"
    /\ ttStop' = TRUE
    /\ drainPc' = "waitTT"
    /\ UNCHANGED <<reqVars, state, stopped, queue, sema, entryVars, waiters,
                   drainQ, drainInflight, ttPc, ttEntry, maxFired, snapCurrent, shPc, errs>>

\* ... tt.wg.Wait()
DrainWaitTT ==
    /\ drainPc = "waitTT"
    /\ ttPc = "none"
    /\ drainPc' = "unblock"
    /\ UNCHANGED <<reqVars, state, stopped, queue, sema, entryVars, waiters,
                   drainQ, drainInflight, ttVars, shPc, errs>>

\* A drain worker takes the next index and calls unblockAndWait(blocking).
DrainUnblock ==
    /\ drainPc = "unblock"
    /\ drainQ # <<>>
    /\ Cardinality(drainInflight) < DrainConcurrency
    /\ LET e == Head(drainQ) IN
        /\ done' = [done EXCEPT ![e] = TRUE]
        /\ errs' = errs \cup CloseErr(e)
        /\ drainInflight' = drainInflight \cup {e}
        /\ drainQ' = Tail(drainQ)
    /\ UNCHANGED <<reqVars, state, stopped, queue, sema, bufCtxDone, ctxCanceled,
                   expired, waiters, drainPc, ttVars, shPc>>

\* The worker's waitForRequestFinish(releaseSlot=true) returns.
DrainRelease(e) ==
    /\ e \in drainInflight
    /\ bufCtxDone[e]
    /\ drainInflight' = drainInflight \ {e}
    /\ ReleaseSema
    /\ UNCHANGED <<reqVars, state, stopped, queue, entryVars, waiters,
                   drainPc, drainQ, ttVars, shPc>>

\* wg.Wait() returned; under sb.mu: state = idle, timeoutThread = nil.
DrainFinish ==
    /\ drainPc = "unblock"
    /\ drainQ = <<>>
    /\ drainInflight = {}
    /\ state' = "idle"
    /\ drainPc' = "none"
    /\ errs' = errs \cup (IF state # "draining" THEN {"drain finished in wrong state"} ELSE {})
    /\ UNCHANGED <<reqVars, stopped, queue, sema, entryVars, waiters,
                   drainQ, drainInflight, ttVars, shPc>>

-----------------------------------------------------------------------------
(* Buffer.Shutdown() *)
ShutdownBegin ==
    /\ shPc = "none"
    /\ stopped' = TRUE
    /\ shPc' = "stopping"
    /\ UNCHANGED <<reqVars, state, queue, sema, entryVars, waiters, drainVars, ttVars, errs>>

ShutdownStopShard ==
    /\ shPc = "stopping"
    /\ shPc' = "waiting"
    /\ StopBufferingLocked
    /\ UNCHANGED <<reqVars, stopped, sema, entryVars, waiters, ttVars>>

\* waitForShutdown(): sb.wg.Wait() returns once the counter reaches zero.
ShutdownWait ==
    /\ shPc = "waiting"
    /\ WG = 0
    /\ shPc' = "done"
    /\ UNCHANGED <<reqVars, state, stopped, queue, sema, entryVars, waiters,
                   drainVars, ttVars, errs>>

-----------------------------------------------------------------------------
ReqStep(r) == ReqStart(r) \/ ReqSlow(r) \/ ReqWait(r) \/ ReqRemove(r)
              \/ ReqRetry(r) \/ ReqEnd(r)

TTStep == TTLoop \/ TTCheck \/ TTSnap \/ TTWaitNE \/ TTWaitEntry \/ TTEvict \/ TTStopBuf

DrainStep == DrainStopTT \/ DrainWaitTT \/ DrainUnblock \/ DrainFinish
             \/ \E e \in Entries : DrainRelease(e)

Next ==
    \/ \E r \in Req : ReqStep(r) \/ ReqCancel(r)
    \/ Expire
    \/ \E w \in waiters : WaiterFinish(w)
    \/ TTStep
    \/ MaxDurationFires
    \/ KeyspaceEvent
    \/ DrainStep
    \/ ShutdownBegin \/ ShutdownStopShard \/ ShutdownWait

\* Every goroutine that can run eventually runs. External events (client
\* cancellation, keyspace events, Shutdown) are not forced to happen.
Fairness ==
    /\ \A r \in Req : WF_vars(ReqStep(r))
    /\ WF_vars(Expire)
    /\ \A e \in Entries : \A rel \in BOOLEAN :
          WF_vars(([e |-> e, rel |-> rel] \in waiters) /\ WaiterFinish([e |-> e, rel |-> rel]))
    /\ WF_vars(TTStep)
    /\ WF_vars(DrainStep)
    /\ WF_vars(ShutdownStopShard \/ ShutdownWait)
    /\ (FairMaxDuration => WF_vars(MaxDurationFires))

Spec == Init /\ [][Next]_vars /\ Fairness

\* Requests are interchangeable (used for safety checking only).
Symmetry == Permutations(Req)

-----------------------------------------------------------------------------
(* Safety properties *)

\* No panic (double close, semaphore over-release) and no WaitGroup misuse.
NoRuntimeErrors == errs = {}

TypeOK ==
    /\ state \in {"idle", "buffering", "draining"}
    /\ sema \in 0..Size
    /\ \A i \in 1..Len(queue) : queue[i] \in Entries

\* Every slot is either free, held by a queued entry, or held by an unblocked
\* request whose release is still pending. Nothing leaks, nothing is minted.
SlotConservation ==
    sema + Len(queue) + Cardinality({w \in waiters : w.rel})
         + Len(drainQ) + Cardinality(drainInflight) = Size

\* The queue only holds blocked requests, and it is empty unless buffering.
QueueWellFormed ==
    /\ state # "buffering" => queue = <<>>
    /\ \A i \in 1..Len(queue) :
         LET e == queue[i] IN
         /\ ~done[e]
         /\ att[e[1]] = e[2]
         /\ pc[e[1]] \in {"wait", "remove"}

\* While buffering, a live timeout thread polices the queue.
BufferingHasTimeoutThread ==
    state = "buffering" => (ttPc # "none" /\ ~ttStop)

\* The timeout thread never sleeps on an already-replaced (still open)
\* queueNotEmpty channel while there are entries whose window it must enforce.
\* If this is violated, no further notifyQueueNotEmpty() will arrive (it only
\* fires on the empty->non-empty transition) and buffered requests are held
\* past --buffer-window until max-failover-duration or the failover ends.
TimeoutThreadNotBlind ==
    (ttPc = "waitNE" /\ snapCurrent /\ ~maxFired /\ ~ttStop) => queue = <<>>

\* After Shutdown() returns nothing is running and buffering is over for good.
ShutdownIsFinal ==
    shPc = "done" =>
        /\ state = "idle"
        /\ ttPc = "none"
        /\ drainPc = "none"
        /\ waiters = {}

-----------------------------------------------------------------------------
(* Liveness properties *)

\* Every request goroutine eventually completes all its calls.
AllRequestsComplete == \A r \in Req : <>(pc[r] = "fin")

\* An entry whose buffering window expired is eventually unblocked (or the
\* failover ends), without relying on the max-failover-duration timer.
WindowEnforced ==
    \A e \in Entries :
        (state = "buffering" /\ queue # <<>> /\ Head(queue) = e /\ expired[e])
            ~> (done[e] \/ state # "buffering")

\* Shutdown() eventually returns once it starts.
ShutdownCompletes == (shPc # "none") ~> (shPc = "done")
=============================================================================
