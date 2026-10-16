---- MODULE MoveTablesSwitchWrites_TTrace_1792158269 ----
EXTENDS MoveTablesSwitchWrites, Sequences, TLCExt, Toolbox, Naturals, TLC, MoveTablesSwitchWrites_TEConstants

_expression ==
    LET MoveTablesSwitchWrites_TEExpression == INSTANCE MoveTablesSwitchWrites_TEExpression
    IN MoveTablesSwitchWrites_TEExpression!expression
----

_trace ==
    LET MoveTablesSwitchWrites_TETrace == INSTANCE MoveTablesSwitchWrites_TETrace
    IN MoveTablesSwitchWrites_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        streamOn = (FALSE)
        /\
        rs = ((r1 :> "acked" @@ r2 :> "idle"))
        /\
        drainQ = ({})
        /\
        deniedAtTgt = (FALSE)
        /\
        lastEndRecent = (FALSE)
        /\
        epoch = (1)
        /\
        releasedEarly = (FALSE)
        /\
        capVer = ((r1 :> 0 @@ r2 :> 0))
        /\
        dest = ((r1 :> "src" @@ r2 :> "src"))
        /\
        tgtTabDeny = (FALSE)
        /\
        bufQ = ({})
        /\
        srcTopoDeny = (TRUE)
        /\
        extra = (0)
        /\
        kewMT = ("none")
        /\
        bufOnce = ((r1 :> FALSE @@ r2 :> FALSE))
        /\
        replPos = (0)
        /\
        startedAfter = ((r1 :> FALSE @@ r2 :> FALSE))
        /\
        kewVer = (1)
        /\
        bufState = ("idle")
        /\
        lastVer = ((r1 :> 0 @@ r2 :> 0))
        /\
        failedAfterSwitch = (FALSE)
        /\
        srcTabDeny = (TRUE)
        /\
        srcPos = (0)
        /\
        cache = ([ep |-> 1, route |-> "src"])
        /\
        sw = ([srcTopo |-> TRUE, srcApplied |-> TRUE, srcRef |-> TRUE, tgtTopo |-> TRUE, tgtRef |-> TRUE])
        /\
        tgtTopoDeny = (FALSE)
        /\
        tgtClient = ({})
        /\
        kewCons = (TRUE)
        /\
        srcLog = (<<r1>>)
        /\
        vtVer = (1)
        /\
        tgtData = ({})
        /\
        routeHist = (<<"src">>)
        /\
        pc = ("journal")
        /\
        kewServing = (TRUE)
        /\
        gwErr = ((r1 :> FALSE @@ r2 :> FALSE))
        /\
        invalid = ((r1 :> FALSE @@ r2 :> FALSE))
        /\
        try = ((r1 :> 0 @@ r2 :> 0))
        /\
        kewEvent = (FALSE)
    )
----

_init ==
    /\ srcLog = _TETrace[1].srcLog
    /\ startedAfter = _TETrace[1].startedAfter
    /\ epoch = _TETrace[1].epoch
    /\ extra = _TETrace[1].extra
    /\ bufOnce = _TETrace[1].bufOnce
    /\ srcTabDeny = _TETrace[1].srcTabDeny
    /\ srcPos = _TETrace[1].srcPos
    /\ tgtData = _TETrace[1].tgtData
    /\ srcTopoDeny = _TETrace[1].srcTopoDeny
    /\ kewCons = _TETrace[1].kewCons
    /\ bufState = _TETrace[1].bufState
    /\ routeHist = _TETrace[1].routeHist
    /\ streamOn = _TETrace[1].streamOn
    /\ pc = _TETrace[1].pc
    /\ kewMT = _TETrace[1].kewMT
    /\ dest = _TETrace[1].dest
    /\ kewVer = _TETrace[1].kewVer
    /\ vtVer = _TETrace[1].vtVer
    /\ lastEndRecent = _TETrace[1].lastEndRecent
    /\ tgtTabDeny = _TETrace[1].tgtTabDeny
    /\ rs = _TETrace[1].rs
    /\ tgtTopoDeny = _TETrace[1].tgtTopoDeny
    /\ try = _TETrace[1].try
    /\ failedAfterSwitch = _TETrace[1].failedAfterSwitch
    /\ sw = _TETrace[1].sw
    /\ releasedEarly = _TETrace[1].releasedEarly
    /\ kewServing = _TETrace[1].kewServing
    /\ replPos = _TETrace[1].replPos
    /\ bufQ = _TETrace[1].bufQ
    /\ capVer = _TETrace[1].capVer
    /\ tgtClient = _TETrace[1].tgtClient
    /\ deniedAtTgt = _TETrace[1].deniedAtTgt
    /\ gwErr = _TETrace[1].gwErr
    /\ drainQ = _TETrace[1].drainQ
    /\ lastVer = _TETrace[1].lastVer
    /\ kewEvent = _TETrace[1].kewEvent
    /\ cache = _TETrace[1].cache
    /\ invalid = _TETrace[1].invalid
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ srcLog  = _TETrace[i].srcLog
        /\ srcLog' = _TETrace[j].srcLog
        /\ startedAfter  = _TETrace[i].startedAfter
        /\ startedAfter' = _TETrace[j].startedAfter
        /\ epoch  = _TETrace[i].epoch
        /\ epoch' = _TETrace[j].epoch
        /\ extra  = _TETrace[i].extra
        /\ extra' = _TETrace[j].extra
        /\ bufOnce  = _TETrace[i].bufOnce
        /\ bufOnce' = _TETrace[j].bufOnce
        /\ srcTabDeny  = _TETrace[i].srcTabDeny
        /\ srcTabDeny' = _TETrace[j].srcTabDeny
        /\ srcPos  = _TETrace[i].srcPos
        /\ srcPos' = _TETrace[j].srcPos
        /\ tgtData  = _TETrace[i].tgtData
        /\ tgtData' = _TETrace[j].tgtData
        /\ srcTopoDeny  = _TETrace[i].srcTopoDeny
        /\ srcTopoDeny' = _TETrace[j].srcTopoDeny
        /\ kewCons  = _TETrace[i].kewCons
        /\ kewCons' = _TETrace[j].kewCons
        /\ bufState  = _TETrace[i].bufState
        /\ bufState' = _TETrace[j].bufState
        /\ routeHist  = _TETrace[i].routeHist
        /\ routeHist' = _TETrace[j].routeHist
        /\ streamOn  = _TETrace[i].streamOn
        /\ streamOn' = _TETrace[j].streamOn
        /\ pc  = _TETrace[i].pc
        /\ pc' = _TETrace[j].pc
        /\ kewMT  = _TETrace[i].kewMT
        /\ kewMT' = _TETrace[j].kewMT
        /\ dest  = _TETrace[i].dest
        /\ dest' = _TETrace[j].dest
        /\ kewVer  = _TETrace[i].kewVer
        /\ kewVer' = _TETrace[j].kewVer
        /\ vtVer  = _TETrace[i].vtVer
        /\ vtVer' = _TETrace[j].vtVer
        /\ lastEndRecent  = _TETrace[i].lastEndRecent
        /\ lastEndRecent' = _TETrace[j].lastEndRecent
        /\ tgtTabDeny  = _TETrace[i].tgtTabDeny
        /\ tgtTabDeny' = _TETrace[j].tgtTabDeny
        /\ rs  = _TETrace[i].rs
        /\ rs' = _TETrace[j].rs
        /\ tgtTopoDeny  = _TETrace[i].tgtTopoDeny
        /\ tgtTopoDeny' = _TETrace[j].tgtTopoDeny
        /\ try  = _TETrace[i].try
        /\ try' = _TETrace[j].try
        /\ failedAfterSwitch  = _TETrace[i].failedAfterSwitch
        /\ failedAfterSwitch' = _TETrace[j].failedAfterSwitch
        /\ sw  = _TETrace[i].sw
        /\ sw' = _TETrace[j].sw
        /\ releasedEarly  = _TETrace[i].releasedEarly
        /\ releasedEarly' = _TETrace[j].releasedEarly
        /\ kewServing  = _TETrace[i].kewServing
        /\ kewServing' = _TETrace[j].kewServing
        /\ replPos  = _TETrace[i].replPos
        /\ replPos' = _TETrace[j].replPos
        /\ bufQ  = _TETrace[i].bufQ
        /\ bufQ' = _TETrace[j].bufQ
        /\ capVer  = _TETrace[i].capVer
        /\ capVer' = _TETrace[j].capVer
        /\ tgtClient  = _TETrace[i].tgtClient
        /\ tgtClient' = _TETrace[j].tgtClient
        /\ deniedAtTgt  = _TETrace[i].deniedAtTgt
        /\ deniedAtTgt' = _TETrace[j].deniedAtTgt
        /\ gwErr  = _TETrace[i].gwErr
        /\ gwErr' = _TETrace[j].gwErr
        /\ drainQ  = _TETrace[i].drainQ
        /\ drainQ' = _TETrace[j].drainQ
        /\ lastVer  = _TETrace[i].lastVer
        /\ lastVer' = _TETrace[j].lastVer
        /\ kewEvent  = _TETrace[i].kewEvent
        /\ kewEvent' = _TETrace[j].kewEvent
        /\ cache  = _TETrace[i].cache
        /\ cache' = _TETrace[j].cache
        /\ invalid  = _TETrace[i].invalid
        /\ invalid' = _TETrace[j].invalid

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("MoveTablesSwitchWrites_TTrace_1792158269.json", _TETrace)

=============================================================================

 Note that you can extract this module `MoveTablesSwitchWrites_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `MoveTablesSwitchWrites_TEExpression.tla` file takes precedence 
  over the module `MoveTablesSwitchWrites_TEExpression` below).

---- MODULE MoveTablesSwitchWrites_TEExpression ----
EXTENDS MoveTablesSwitchWrites, Sequences, TLCExt, Toolbox, Naturals, TLC, MoveTablesSwitchWrites_TEConstants

expression == 
    [
        \* To hide variables of the `MoveTablesSwitchWrites` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        srcLog |-> srcLog
        ,startedAfter |-> startedAfter
        ,epoch |-> epoch
        ,extra |-> extra
        ,bufOnce |-> bufOnce
        ,srcTabDeny |-> srcTabDeny
        ,srcPos |-> srcPos
        ,tgtData |-> tgtData
        ,srcTopoDeny |-> srcTopoDeny
        ,kewCons |-> kewCons
        ,bufState |-> bufState
        ,routeHist |-> routeHist
        ,streamOn |-> streamOn
        ,pc |-> pc
        ,kewMT |-> kewMT
        ,dest |-> dest
        ,kewVer |-> kewVer
        ,vtVer |-> vtVer
        ,lastEndRecent |-> lastEndRecent
        ,tgtTabDeny |-> tgtTabDeny
        ,rs |-> rs
        ,tgtTopoDeny |-> tgtTopoDeny
        ,try |-> try
        ,failedAfterSwitch |-> failedAfterSwitch
        ,sw |-> sw
        ,releasedEarly |-> releasedEarly
        ,kewServing |-> kewServing
        ,replPos |-> replPos
        ,bufQ |-> bufQ
        ,capVer |-> capVer
        ,tgtClient |-> tgtClient
        ,deniedAtTgt |-> deniedAtTgt
        ,gwErr |-> gwErr
        ,drainQ |-> drainQ
        ,lastVer |-> lastVer
        ,kewEvent |-> kewEvent
        ,cache |-> cache
        ,invalid |-> invalid
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_srcLogUnchanged |-> srcLog = srcLog'
        
        \* Format the `srcLog` variable as Json value.
        \* ,_srcLogJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(srcLog)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_srcLogModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].srcLog # _TETrace[s-1].srcLog
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE MoveTablesSwitchWrites_TETrace ----
\*EXTENDS MoveTablesSwitchWrites, IOUtils, TLC, MoveTablesSwitchWrites_TEConstants
\*
\*trace == IODeserialize("MoveTablesSwitchWrites_TTrace_1792158269.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE MoveTablesSwitchWrites_TETrace ----
EXTENDS MoveTablesSwitchWrites, TLC, MoveTablesSwitchWrites_TEConstants

trace == 
    <<
    ([streamOn |-> TRUE,rs |-> (r1 :> "idle" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 0 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> TRUE,bufQ |-> {},srcTopoDeny |-> FALSE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 0 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> FALSE,srcPos |-> 0,cache |-> [ep |-> 0, route |-> "src"],sw |-> [srcTopo |-> FALSE, srcApplied |-> FALSE, srcRef |-> FALSE, tgtTopo |-> FALSE, tgtRef |-> FALSE],tgtTopoDeny |-> TRUE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "init",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE]),
    ([streamOn |-> TRUE,rs |-> (r1 :> "idle" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 0 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> TRUE,bufQ |-> {},srcTopoDeny |-> FALSE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 0 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> FALSE,srcPos |-> 0,cache |-> [ep |-> 0, route |-> "src"],sw |-> [srcTopo |-> FALSE, srcApplied |-> FALSE, srcRef |-> FALSE, tgtTopo |-> FALSE, tgtRef |-> FALSE],tgtTopoDeny |-> TRUE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "stopWrites",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE]),
    ([streamOn |-> TRUE,rs |-> (r1 :> "idle" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 0 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> TRUE,bufQ |-> {},srcTopoDeny |-> TRUE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 0 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> FALSE,srcPos |-> 0,cache |-> [ep |-> 0, route |-> "src"],sw |-> [srcTopo |-> TRUE, srcApplied |-> FALSE, srcRef |-> FALSE, tgtTopo |-> FALSE, tgtRef |-> FALSE],tgtTopoDeny |-> TRUE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "stopWrites",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE]),
    ([streamOn |-> TRUE,rs |-> (r1 :> "idle" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 0 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> TRUE,bufQ |-> {},srcTopoDeny |-> TRUE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 0 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> FALSE,srcPos |-> 0,cache |-> [ep |-> 0, route |-> "src"],sw |-> [srcTopo |-> TRUE, srcApplied |-> FALSE, srcRef |-> FALSE, tgtTopo |-> TRUE, tgtRef |-> FALSE],tgtTopoDeny |-> FALSE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "stopWrites",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE]),
    ([streamOn |-> TRUE,rs |-> (r1 :> "idle" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 0 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> FALSE,bufQ |-> {},srcTopoDeny |-> TRUE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 0 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> FALSE,srcPos |-> 0,cache |-> [ep |-> 0, route |-> "src"],sw |-> [srcTopo |-> TRUE, srcApplied |-> FALSE, srcRef |-> FALSE, tgtTopo |-> TRUE, tgtRef |-> TRUE],tgtTopoDeny |-> FALSE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "stopWrites",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE]),
    ([streamOn |-> TRUE,rs |-> (r1 :> "capture" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 0 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> FALSE,bufQ |-> {},srcTopoDeny |-> TRUE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 1 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> FALSE,srcPos |-> 0,cache |-> [ep |-> 0, route |-> "src"],sw |-> [srcTopo |-> TRUE, srcApplied |-> FALSE, srcRef |-> FALSE, tgtTopo |-> TRUE, tgtRef |-> TRUE],tgtTopoDeny |-> FALSE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "stopWrites",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE]),
    ([streamOn |-> TRUE,rs |-> (r1 :> "load" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 1 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> FALSE,bufQ |-> {},srcTopoDeny |-> TRUE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 1 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> FALSE,srcPos |-> 0,cache |-> [ep |-> 0, route |-> "src"],sw |-> [srcTopo |-> TRUE, srcApplied |-> FALSE, srcRef |-> FALSE, tgtTopo |-> TRUE, tgtRef |-> TRUE],tgtTopoDeny |-> FALSE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "stopWrites",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE]),
    ([streamOn |-> TRUE,rs |-> (r1 :> "gwBuf" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 0 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> FALSE,bufQ |-> {},srcTopoDeny |-> TRUE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 1 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> FALSE,srcPos |-> 0,cache |-> [ep |-> 1, route |-> "src"],sw |-> [srcTopo |-> TRUE, srcApplied |-> FALSE, srcRef |-> FALSE, tgtTopo |-> TRUE, tgtRef |-> TRUE],tgtTopoDeny |-> FALSE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "stopWrites",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE]),
    ([streamOn |-> TRUE,rs |-> (r1 :> "send" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 0 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> FALSE,bufQ |-> {},srcTopoDeny |-> TRUE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 1 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> FALSE,srcPos |-> 0,cache |-> [ep |-> 1, route |-> "src"],sw |-> [srcTopo |-> TRUE, srcApplied |-> FALSE, srcRef |-> FALSE, tgtTopo |-> TRUE, tgtRef |-> TRUE],tgtTopoDeny |-> FALSE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "stopWrites",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE]),
    ([streamOn |-> TRUE,rs |-> (r1 :> "passed" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 0 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> FALSE,bufQ |-> {},srcTopoDeny |-> TRUE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 1 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> FALSE,srcPos |-> 0,cache |-> [ep |-> 1, route |-> "src"],sw |-> [srcTopo |-> TRUE, srcApplied |-> FALSE, srcRef |-> FALSE, tgtTopo |-> TRUE, tgtRef |-> TRUE],tgtTopoDeny |-> FALSE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "stopWrites",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE]),
    ([streamOn |-> TRUE,rs |-> (r1 :> "passed" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 0 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> FALSE,bufQ |-> {},srcTopoDeny |-> TRUE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 1 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> TRUE,srcPos |-> 0,cache |-> [ep |-> 1, route |-> "src"],sw |-> [srcTopo |-> TRUE, srcApplied |-> TRUE, srcRef |-> TRUE, tgtTopo |-> TRUE, tgtRef |-> TRUE],tgtTopoDeny |-> FALSE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "stopWrites",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE]),
    ([streamOn |-> TRUE,rs |-> (r1 :> "passed" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 0 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> FALSE,bufQ |-> {},srcTopoDeny |-> TRUE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 1 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> TRUE,srcPos |-> 0,cache |-> [ep |-> 1, route |-> "src"],sw |-> [srcTopo |-> TRUE, srcApplied |-> TRUE, srcRef |-> TRUE, tgtTopo |-> TRUE, tgtRef |-> TRUE],tgtTopoDeny |-> FALSE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "lock1",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE]),
    ([streamOn |-> TRUE,rs |-> (r1 :> "passed" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 0 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> FALSE,bufQ |-> {},srcTopoDeny |-> TRUE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 1 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> TRUE,srcPos |-> 0,cache |-> [ep |-> 1, route |-> "src"],sw |-> [srcTopo |-> TRUE, srcApplied |-> TRUE, srcRef |-> TRUE, tgtTopo |-> TRUE, tgtRef |-> TRUE],tgtTopoDeny |-> FALSE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "lock2",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE]),
    ([streamOn |-> TRUE,rs |-> (r1 :> "passed" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 0 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> FALSE,bufQ |-> {},srcTopoDeny |-> TRUE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 1 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> TRUE,srcPos |-> 0,cache |-> [ep |-> 1, route |-> "src"],sw |-> [srcTopo |-> TRUE, srcApplied |-> TRUE, srcRef |-> TRUE, tgtTopo |-> TRUE, tgtRef |-> TRUE],tgtTopoDeny |-> FALSE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "gather",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE]),
    ([streamOn |-> TRUE,rs |-> (r1 :> "passed" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 0 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> FALSE,bufQ |-> {},srcTopoDeny |-> TRUE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 1 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> TRUE,srcPos |-> 0,cache |-> [ep |-> 1, route |-> "src"],sw |-> [srcTopo |-> TRUE, srcApplied |-> TRUE, srcRef |-> TRUE, tgtTopo |-> TRUE, tgtRef |-> TRUE],tgtTopoDeny |-> FALSE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "catchup",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE]),
    ([streamOn |-> TRUE,rs |-> (r1 :> "mdl" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 0 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> FALSE,bufQ |-> {},srcTopoDeny |-> TRUE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 1 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> TRUE,srcPos |-> 0,cache |-> [ep |-> 1, route |-> "src"],sw |-> [srcTopo |-> TRUE, srcApplied |-> TRUE, srcRef |-> TRUE, tgtTopo |-> TRUE, tgtRef |-> TRUE],tgtTopoDeny |-> FALSE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "catchup",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE]),
    ([streamOn |-> FALSE,rs |-> (r1 :> "mdl" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 0 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> FALSE,bufQ |-> {},srcTopoDeny |-> TRUE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 1 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> TRUE,srcPos |-> 0,cache |-> [ep |-> 1, route |-> "src"],sw |-> [srcTopo |-> TRUE, srcApplied |-> TRUE, srcRef |-> TRUE, tgtTopo |-> TRUE, tgtRef |-> TRUE],tgtTopoDeny |-> FALSE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "reverse",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE]),
    ([streamOn |-> FALSE,rs |-> (r1 :> "mdl" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 0 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> FALSE,bufQ |-> {},srcTopoDeny |-> TRUE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 1 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> TRUE,srcPos |-> 0,cache |-> [ep |-> 1, route |-> "src"],sw |-> [srcTopo |-> TRUE, srcApplied |-> TRUE, srcRef |-> TRUE, tgtTopo |-> TRUE, tgtRef |-> TRUE],tgtTopoDeny |-> FALSE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "journal",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE]),
    ([streamOn |-> FALSE,rs |-> (r1 :> "acked" @@ r2 :> "idle"),drainQ |-> {},deniedAtTgt |-> FALSE,lastEndRecent |-> FALSE,epoch |-> 1,releasedEarly |-> FALSE,capVer |-> (r1 :> 0 @@ r2 :> 0),dest |-> (r1 :> "src" @@ r2 :> "src"),tgtTabDeny |-> FALSE,bufQ |-> {},srcTopoDeny |-> TRUE,extra |-> 0,kewMT |-> "none",bufOnce |-> (r1 :> FALSE @@ r2 :> FALSE),replPos |-> 0,startedAfter |-> (r1 :> FALSE @@ r2 :> FALSE),kewVer |-> 1,bufState |-> "idle",lastVer |-> (r1 :> 0 @@ r2 :> 0),failedAfterSwitch |-> FALSE,srcTabDeny |-> TRUE,srcPos |-> 0,cache |-> [ep |-> 1, route |-> "src"],sw |-> [srcTopo |-> TRUE, srcApplied |-> TRUE, srcRef |-> TRUE, tgtTopo |-> TRUE, tgtRef |-> TRUE],tgtTopoDeny |-> FALSE,tgtClient |-> {},kewCons |-> TRUE,srcLog |-> <<r1>>,vtVer |-> 1,tgtData |-> {},routeHist |-> <<"src">>,pc |-> "journal",kewServing |-> TRUE,gwErr |-> (r1 :> FALSE @@ r2 :> FALSE),invalid |-> (r1 :> FALSE @@ r2 :> FALSE),try |-> (r1 :> 0 @@ r2 :> 0),kewEvent |-> FALSE])
    >>
----


=============================================================================

---- MODULE MoveTablesSwitchWrites_TEConstants ----
EXTENDS MoveTablesSwitchWrites

CONSTANTS r1, r2

=============================================================================

---- CONFIG MoveTablesSwitchWrites_TTrace_1792158269 ----
CONSTANTS
    Reqs = { r1 , r2 }
    Buffering = FALSE
    Timeouts = TRUE
    SlowWrites = TRUE
    Force = FALSE
    AllowCancel = FALSE
    MaxExtraRebuilds = 0
    FixedPlanCache = FALSE
    DrainOnDeny = FALSE
    KewFix = FALSE
    Diagnostics = FALSE
    r1 = r1
    r2 = r2

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Fri Oct 16 13:44:30 UTC 2026