-------------------------------- MODULE Fast --------------------------------

EXTENDS Naturals, Bags, FiniteSets, Sequences, TLC, Integers

\* The set of server IDs
CONSTANTS Server, Client

\* The set of requests that can go into the log
CONSTANTS Value

\* Server states.
CONSTANTS Follower, Candidate, Leader
\* Server xraft states.
CONSTANTS FastPrepare, Merge, Fast, Slow  \* 'FastPrepare' is only Leader state 


CONSTANTS COMMITTED, COMMITTED_SYNCED, UNCOMMITTED

\* A reserved key.
CONSTANTS Nil

\* Message types:
CONSTANTS RequestVoteRequest, RequestVoteResponse,
          AppendEntriesRequest, AppendEntriesResponse,
          PrepareToFastRequest, PrepareToFastResponse,
          ClientRequest, ClientAbort, ClientResponse, 
          RaftLogEntry,
          MergeRequest

\* Request type
CONSTANT READ, WRITE

\* Request reply status
CONSTANT FAST_SUCCESS, FAST_CONFLICT, SLOW_SUCCESS

\* Used for filtering messages under different circumstance
CONSTANTS EqualTerm, LessOrEqualTerm

\* Limiting state space by limiting the number of elections and restarts           
CONSTANTS MaxElections, MaxRestarts

CONSTANTS MaxReqs
----
\* Global variables



\* A bag of records representing requests and responses sent from one server
\* to another.
VARIABLE messages

----
\* Auxilliary variables (used for state-space control, invariants etc)

\* The values that have been received from a client and whether
\* the value has been acked back to the client. Used in invariants to
\* detect data loss.
VARIABLE acked
\* Counter for elections and restarts (to control state space)
VARIABLE electionCtr, restartCtr
auxVars == <<acked, electionCtr, restartCtr>>
----

----
\* The following variables are all per server (functions with domain Server).

\* The server's term number.
VARIABLE currentTerm
\* The server's state (Follower, Candidate, or Leader).
VARIABLE state

\* The candidate the server voted for in its current term, or
\* Nil if it hasn't voted for any.
VARIABLE votedFor
serverVars == <<currentTerm, state, votedFor>>





\* A Sequence of log entries. The index into this sequence is the index of the
\* log entry. Unfortunately, the Sequence module defines Head(s) as the entry
\* with index 1, so be careful not to use that!
VARIABLE log
\* The index of the latest entry in the log the state machine may apply.
VARIABLE commitIndex
raftLogVars == <<log, commitIndex>>

\* The following variables are used only on candidates:

\* The set of servers from which the candidate has received a vote in its
\* currentTerm.
VARIABLE votesGranted
candidateVars == <<votesGranted>>

\* The following variables are used only on leaders:
\* The next entry to send to each follower.
VARIABLE nextIndex
\* The latest entry that each follower has acknowledged is the same as the
\* leader's. This is used to calculate commitIndex on the leader.
VARIABLE matchIndex
VARIABLE pendingResponse
leaderVars == <<nextIndex, matchIndex, pendingResponse>>


\* The server's fast state (FastPrepare, Fast, Merge, Slow).
VARIABLE faststate
\* server's fast term number.
VARIABLE fastTerm
VARIABLE fastlog
VARIABLE committedLog  \* committedLog[i][v] <<c1, c2, c3>> 
xraftServerVars == <<faststate, fastTerm, fastlog, committedLog>>


\* End of per server variables.

\* xRaft Client status
VARIABLE clientKonwFTerm \* client知道的最大的fast term
\* A sequence of log entries in clientLeader.
VARIABLE clientLog
\* variables used for client
VARIABLE fIndex
\* used to commit client log
VARIABLE clientLogResponded 
VARIABLE clientLogState
VARIABLE clientApplied, clientCanSend
\* xRaft Client status ends
clientVars == <<clientLog, fIndex, clientLogResponded, clientLogState, clientApplied,clientCanSend, clientKonwFTerm>>

----

\* All variables; used for stuttering (asserting state hasn't changed).
vars == <<messages, serverVars, candidateVars, leaderVars, raftLogVars, clientVars, xraftServerVars, auxVars>>
view == <<messages, serverVars, candidateVars, leaderVars, raftLogVars >>
symmServers == Permutations(Server)
symmValues == Permutations(Value)

----
\* Helpers

\* The set of all quorums. This just calculates simple majorities, but the only
\* important property is that every quorum overlaps with every other.
Quorum == {i \in SUBSET(Server) : Cardinality(i) * 2 > Cardinality(Server)}

\* The term of the last entry in a log, or 0 if the log is empty.
LastTerm(xlog) == IF Len(xlog) = 0 THEN 0 ELSE xlog[Len(xlog)].term

\* Send the message whether it already exists or not.
_SendNoRestriction(m) ==
    IF m \in DOMAIN messages
    THEN messages' = [messages EXCEPT ![m] = @ + 1]
    ELSE messages' = messages @@ (m :> 1)
    
\* Will only send the message if it hasn't been sent before.
\* Basically disables the parent action once sent.    
_SendOnce(m) ==
    /\ m \notin DOMAIN messages
    /\ messages' = messages @@ (m :> 1)    

\* Add a message to the bag of messages. 
\* Note 1: to prevent infinite cycles, empty 
\* AppendEntriesRequest messages can only be sent once.
\* Note 2: a message can only match an existing message
\* if it is identical (all fields).
Send(m) ==
    IF /\ m.mtype = AppendEntriesRequest
       /\ m.mentries = <<>>
    THEN _SendOnce(m)
    ELSE _SendNoRestriction(m)

\* Will only send the messages if it hasn't done so before
\* Basically disables the parent action once sent.
SendMultipleOnce(msgs) ==
    /\ \A m \in msgs : m \notin DOMAIN messages
    /\ messages' = messages @@ [msg \in msgs |-> 1]    

\* Explicit duplicate operator for when we purposefully want message duplication
Duplicate(m) == 
    /\ m \in DOMAIN messages
    /\ messages' = [messages EXCEPT ![m] = @ + 1]

\* Remove a message from the bag of messages. Used when a server is done
\* processing a message.
Discard(m) ==
    /\ m \in DOMAIN messages
    /\ messages[m] > 0 \* message must exist
    /\ messages' = [messages EXCEPT ![m] = @ - 1]

\* Combination of Send and Discard
Reply(response, request) ==
    /\ messages[request] > 0 \* request must exist
    /\ \/ /\ response \notin DOMAIN messages \* response does not exist, so add it
          /\ messages' = [messages EXCEPT ![request] = @ - 1] @@ (response :> 1)
       \/ /\ response \in DOMAIN messages \* response was sent previously, so increment delivery counter
          /\ messages' = [messages EXCEPT ![request] = @ - 1,
                                          ![response] = @ + 1]


\* The message is of the type and has a matching term.
\* Messages with a higher term are handled by the
\* action UpdateTerm
ReceivableMessage(m, mtype, term_match) ==
    /\ messages[m] > 0
    /\ m.msource \notin Client  \* server 内部的消息
    /\ m.mtype = mtype
    /\ \/ /\ term_match = EqualTerm
          /\ m.mterm = currentTerm[m.mdest]
       \/ /\ term_match = LessOrEqualTerm
          /\ m.mterm <= currentTerm[m.mdest]

Receivable2PCMessage(m, mtype) ==
    /\ messages[m] > 0
    /\ ~(m.msource \in Server /\ m.mdest \in Server)\* 非server 内部的消息
    /\ m.mtype = mtype

\* Return the minimum value from a set, or undefined if the set is empty.
Min(s) == CHOOSE x \in s : \A y \in s : x <= y
\* Return the maximum value from a set, or undefined if the set is empty.
Max(s) == CHOOSE x \in s : \A y \in s : x >= y

PrintVal(id, exp)  ==  Print(<<id, exp>>, TRUE)

----
\* Define state transitions

\* ACTION: Restart -------------------------------------
\* Server i restarts from stable storage.
\* It loses everything but its currentTerm, votedFor and log.
Restart(i) ==
    /\ restartCtr < MaxRestarts
    /\ state'           = [state EXCEPT ![i] = Follower]
    /\ votesGranted'    = [votesGranted EXCEPT ![i] = {}]
    /\ nextIndex'       = [nextIndex EXCEPT ![i] = [j \in Server |-> 1]]
    /\ matchIndex'      = [matchIndex EXCEPT ![i] = [j \in Server |-> 0]]
    /\ pendingResponse' = [pendingResponse EXCEPT ![i] = [j \in Server |-> FALSE]]
    /\ commitIndex'     = [commitIndex EXCEPT ![i] = 0]
    /\ restartCtr'      = restartCtr + 1
    /\ UNCHANGED <<messages, currentTerm, votedFor, log, acked,electionCtr, clientVars, xraftServerVars>>

                     
\* ACTION: RequestVote
\* Combined Timeout and RequestVote of the original spec to reduce
\* state space.
\* Server i times out and starts a new election. 
\* Sends a RequestVote request to all peers but not itself.
RequestVote(i) ==
    /\ electionCtr < MaxElections 
    /\ state[i] \in {Follower, Candidate}
    /\ state' = [state EXCEPT ![i] = Candidate]
    /\ currentTerm' = [currentTerm EXCEPT ![i] = currentTerm[i] + 1]
    /\ votedFor' = [votedFor EXCEPT ![i] = i] \* votes for itself
    /\ votesGranted'   = [votesGranted EXCEPT ![i] = {i}] \* votes for itself
    /\ electionCtr' = electionCtr + 1
    /\ SendMultipleOnce(
           {[mtype         |-> RequestVoteRequest,
             mterm         |-> currentTerm[i] + 1,
             mlastLogTerm  |-> LastTerm(log[i]),
             mlastLogIndex |-> Len(log[i]),
             msource       |-> i,
             mdest         |-> j] : j \in Server \ {i}})
    /\ UNCHANGED <<acked, leaderVars, raftLogVars, restartCtr, clientVars, xraftServerVars>>


\* ACTION: BecomeLeader -------------------------------------------
\* Candidate i transitions to leader.
BecomeLeader(i) ==
    /\ state[i] = Candidate
    /\ votesGranted[i] \in Quorum
    /\ state'      = [state EXCEPT ![i] = Leader]
    /\ nextIndex'  = [nextIndex EXCEPT ![i] =
                         [j \in Server |-> Len(log[i]) + 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![i] =
                         [j \in Server |-> 0]]
    /\ pendingResponse' = [pendingResponse EXCEPT ![i] =
                                [j \in Server |-> FALSE]]
    /\ UNCHANGED <<messages, currentTerm, votedFor, candidateVars, auxVars, raftLogVars, clientVars, xraftServerVars>>


\* ACTION: AppendEntries ----------------------------------------
\* Leader i sends j an AppendEntries request containing up to 1 entry.
\* While implementations may want to send more than 1 at a time, this spec uses
\* just 1 because it minimizes atomic regions without loss of generality.
AppendEntries(i, j) ==
    /\ i /= j
    /\ state[i] = Leader
    /\ pendingResponse[i][j] = FALSE \* not already waiting for a response
    /\ LET prevLogIndex == nextIndex[i][j] - 1
           prevLogTerm == IF prevLogIndex > 0 THEN
                              log[i][prevLogIndex].term
                          ELSE
                              0
           \* Send up to 1 entry, constrained by the end of the log.
           lastEntry == Min({Len(log[i]), nextIndex[i][j]})
           entries == SubSeq(log[i], nextIndex[i][j], lastEntry)
       IN 
          /\ pendingResponse' = [pendingResponse EXCEPT ![i][j] = TRUE]
          /\ Send([mtype          |-> AppendEntriesRequest,
                   mterm          |-> currentTerm[i],
                   mprevLogIndex  |-> prevLogIndex,
                   mprevLogTerm   |-> prevLogTerm,
                   mentries       |-> entries,
                   mcommitIndex   |-> Min({commitIndex[i], lastEntry}),
                   msource        |-> i,
                   mdest          |-> j])
    /\ UNCHANGED <<serverVars, candidateVars, nextIndex, matchIndex, raftLogVars, auxVars, clientVars, xraftServerVars>>



\* Server i receives an prepareToFast request from leader j with
\* m.mterm <= currentTerm[i]. 
HandlePrepareToFastRequest(i, m) ==
    /\ Assert(faststate[i] # Fast, "Failure of assertion at change state to Slow.")
    /\ Assert(fastTerm[i] + 1 = m.fastTerm, "Failure of assertion at change state to Slow.")
    /\ fastTerm' =  [fastTerm EXCEPT ![i] = fastTerm[i] + 1] 
    /\ faststate' = [faststate EXCEPT ![i] = Fast]
    /\ fastlog' = [fastlog EXCEPT ![i] = Append(fastlog[i], << >> )] 
    \* /\ committedLog' = [committedLog EXCEPT ![i] = Append(committedLog[i], [k \in Value |-> { } ])] 

\* Server i received merge sync from leader j. (Corresponds to apply the raft Log entry)
HandleMergeSyncRequest(i, m) ==
    /\ Assert(m.fastTerm = fastTerm[i], 
               "Failure of assertion at change state to Slow: fterm not match.")
    /\ Assert(faststate[i] # Slow, 
               "Failure of assertion at change state to Slow: state not match.")
    \* /\ LET canMerge == \* 找出m.mentries中是否存在一对冲突log其顺序与committedLog相反
    \*        \A v \in Value: 
    \*            \/ LET 
    \*                 \A index1, index2 \in 1..Len(m.mentries):
    \*                   \/ index1 > index2
    \*                   \/ m.mentries[index1].value # v
    \*                   \/ m.mentries[index2].value # v  
    \*            \/ LET correspondingEntriesExist == 
    \*                     /\ \E idx1 \in 1..Len(committedLog[i][v]): committedLog[i][v][idx1].client = m.mentries[index1].client
    \*                     /\ \E idx2 \in 1..Len(committedLog[i][v]): committedLog[i][v][idx2].client = m.mentries[index2].client
    \*                IN correspondingEntriesExist
    \*                    => /\ \A idx1, idx2 \in 1..Len(committedLog[i][v]):
    \*                          /\ idx1 < idx2
    \*                          /\ committedLog[i][v][idx1].client = m.mentries[index1].client
    \*                          /\ committedLog[i][v][idx2].client = m.mentries[index2].client
    \*                          /\ committedLog[i][v][idx1] # committedLog[i][v][idx2]
    \*                          => index1 < index2
    \*     IN Assert(canMerge, "merge can't be done successfully")
    /\ fastlog' = [fastlog EXCEPT ![i][m.fastTerm] = m.mentries]
    /\ faststate' = [faststate EXCEPT ![i] = Slow]


\* server i execute the prepareToFast and Merge Command (send from j) 
ApplyFastCmds(i, m) == 
       \/  /\ m.type = PrepareToFastRequest
        \*    /\ PrintVal("Apply a PrepareToFastRequest", [server |-> i]) \* 处理prepare fast过程
           /\ HandlePrepareToFastRequest(i, m) 
           /\ UNCHANGED << committedLog >>
       \/  /\ m.type = MergeRequest
        \*    /\ Print("Apply a MergeRequest",TRUE)
           /\ HandleMergeSyncRequest(i,m)  \* 处理merge过程
           /\ UNCHANGED <<fastTerm, committedLog>>
       \/  /\ m.type = RaftLogEntry
           /\ Send([mtype |-> ClientResponse,
                            msuccess        |-> SLOW_SUCCESS,
                            mfindex         |-> m.value.findex,
                            mterm           |-> fastTerm[i],
                            mraftstate      |-> state[i],
                            mdest           |-> m.value.client,
                            msource         |-> i])
           /\ UNCHANGED <<committedLog, fastTerm, fastlog, faststate>>

\* ACTION: AdvanceCommitIndex ---------------------------------
\* Leader i advances its commitIndex.
\* This is done as a separate step from handling AppendEntries responses,
\* in part to minimize atomic regions, and in part so that leaders of
\* single-server clusters are able to mark entries committed.
AdvanceCommitIndex(i) ==
    /\ state[i] = Leader
    /\ LET \* The set of servers that agree up through index.
           Agree(index) == {i} \cup {k \in Server :
                                         /\ matchIndex[i][k] >= index }
           \* The maximum indexes for which a quorum agrees
           agreeIndexes == {index \in 1..Len(log[i]) : 
                                Agree(index) \in Quorum}
           \* New value for commitIndex'[i]
           newCommitIndex ==
              IF /\ agreeIndexes /= {}
                 /\ log[i][Max(agreeIndexes)].term = currentTerm[i]
              THEN
                  Max(agreeIndexes)
              ELSE
                  commitIndex[i]
       IN 
          /\ commitIndex[i] < newCommitIndex \* only enabled if it actually advances
          /\ \A x \in (commitIndex[i]+1)..newCommitIndex: ApplyFastCmds(i, log[i][x])  \* leader 处理xRaft cmds  
          /\ commitIndex' = [commitIndex EXCEPT ![i] = newCommitIndex]
          /\ acked' = [v \in Value |-> [
                       c \in Client |-> 
                        IF acked[v][c] = FALSE
                        THEN [Val |-> v, Client |-> c] \in { IF log[i][index].type = RaftLogEntry THEN  [Val |-> log[i][index].value, Client |-> log[i][index].source ] ELSE [Val |-> Nil, Client |-> Nil] : index \in commitIndex[i]+1..newCommitIndex }
                        ELSE acked[v][c]
                     ]]
    /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, log, pendingResponse, electionCtr, restartCtr, clientVars>>

\* ACTION: HandleRequestVoteRequest ------------------------------
\* Server i receives a RequestVote request from server j with
\* m.mterm <= currentTerm[i].
HandleRequestVoteRequest ==
    \E m \in DOMAIN messages :
        /\ ReceivableMessage(m, RequestVoteRequest, LessOrEqualTerm)
        /\ LET i     == m.mdest
               j     == m.msource
               logOk == \/ m.mlastLogTerm > LastTerm(log[i])
                        \/ /\ m.mlastLogTerm = LastTerm(log[i])
                           /\ m.mlastLogIndex >= Len(log[i])
               grant == /\ m.mterm = currentTerm[i]
                        /\ logOk
                        /\ votedFor[i] \in {Nil, j}
            IN /\ m.mterm <= currentTerm[i]
               /\ \/ grant  /\ votedFor' = [votedFor EXCEPT ![i] = j]
                  \/ ~grant /\ UNCHANGED votedFor
               /\ Reply([mtype        |-> RequestVoteResponse,
                         mterm        |-> currentTerm[i],
                         mvoteGranted |-> grant,
                         msource      |-> i,
                         mdest        |-> j],
                         m)
       /\ UNCHANGED <<state, currentTerm, candidateVars, leaderVars, raftLogVars, auxVars, clientVars, xraftServerVars>> 



\* ACTION: HandleRequestVoteResponse --------------------------------
\* Server i receives a RequestVote response from server j with
\* m.mterm = currentTerm[i].
HandleRequestVoteResponse ==
    \E m \in DOMAIN messages :
        \* This tallies votes even when the current state is not Candidate, but
        \* they won't be looked at, so it doesn't matter.
        /\ ReceivableMessage(m, RequestVoteResponse, EqualTerm)
        /\ LET i     == m.mdest
               j     == m.msource
           IN
              /\ \/ /\ m.mvoteGranted
                    /\ votesGranted' = [votesGranted EXCEPT ![i] =
                                              votesGranted[i] \cup {j}]
                 \/ /\ ~m.mvoteGranted
                    /\ UNCHANGED <<votesGranted>>
              /\ Discard(m)
              /\ UNCHANGED <<serverVars, votedFor, leaderVars, raftLogVars, auxVars, clientVars, xraftServerVars>>

\* ACTION: RejectAppendEntriesRequest -------------------
\* Either the term of the message is stale or the message
\* entry is too high (beyond the last log entry + 1)
LogOk(i, m) ==
    \/ m.mprevLogIndex = 0
    \/ /\ m.mprevLogIndex > 0
       /\ m.mprevLogIndex <= Len(log[i])
       /\ m.mprevLogTerm = log[i][m.mprevLogIndex].term


RejectAppendEntriesRequest ==
    \E m \in DOMAIN messages :
        /\ ReceivableMessage(m, AppendEntriesRequest, LessOrEqualTerm)
        /\ LET i     == m.mdest
               j     == m.msource
               logOk == LogOk(i, m)
           IN  /\ \/ m.mterm < currentTerm[i]
                  \/ /\ m.mterm = currentTerm[i]
                     /\ state[i] = Follower
                     /\ \lnot logOk
               /\ Reply([mtype           |-> AppendEntriesResponse,
                         mterm           |-> currentTerm[i],
                         msuccess        |-> FALSE,
                         mmatchIndex     |-> 0,
                         msource         |-> i,
                         mdest           |-> j],
                         m)
               /\ UNCHANGED <<state, candidateVars, leaderVars, serverVars, 
                              raftLogVars, auxVars, clientVars, xraftServerVars>>



\* ACTION: AcceptAppendEntriesRequest ------------------
\* The original spec had to three sub actions, this version is compressed.
\* In one step it can:
\* - truncate the log
\* - append an entry to the log
\* - respond to the leader         
CanAppend(m, i) ==
    /\ m.mentries /= << >>
    /\ Len(log[i]) = m.mprevLogIndex

\* truncate in two cases:
\* - the last log entry index is >= than the entry being received
\* - this is an empty RPC and the last log entry index is > than the previous log entry received
NeedsTruncation(m, i, index) ==
   \/ /\ m.mentries /= <<>>
      /\ Len(log[i]) >= index
   \/ /\ m.mentries = <<>>
      /\ Len(log[i]) > m.mprevLogIndex

TruncateLog(m, i) ==
    [index \in 1..m.mprevLogIndex |-> log[i][index]]


AcceptAppendEntriesRequest ==
    \E m \in DOMAIN messages :
        /\ ReceivableMessage(m, AppendEntriesRequest, EqualTerm)
        /\ LET i     == m.mdest
               j     == m.msource
               logOk == LogOk(i, m)
               index == m.mprevLogIndex + 1
           IN 
              /\ state[i] \in { Follower, Candidate }
              /\ logOk
              /\ LET new_log == CASE CanAppend(m, i) ->
                                        [log EXCEPT ![i] = Append(log[i], m.mentries[1])]
                                  [] NeedsTruncation(m, i , index) /\ m.mentries # <<>> ->
                                        [log EXCEPT ![i] = Append(TruncateLog(m, i), m.mentries[1])]
                                  [] NeedsTruncation(m, i , index) /\ m.mentries = <<>> ->
                                        [log EXCEPT ![i] = TruncateLog(m, i)]
                                  [] OTHER -> log 
                 IN
                    /\ state' = [state EXCEPT ![i] = Follower]
                    /\ \/ /\ m.mcommitIndex > commitIndex[i]
                          /\ \A x \in (commitIndex[i]+1)..m.mcommitIndex: ApplyFastCmds(i, new_log[i][x])  \* 处理xRaft cmds  
                       \/ /\  m.mcommitIndex <= commitIndex[i]
                          /\ UNCHANGED <<committedLog, fastTerm, fastlog, faststate>>
                    /\ commitIndex' = [commitIndex EXCEPT ![i] =
                                              m.mcommitIndex]
                    /\ log' = new_log
                    /\ Reply([mtype           |-> AppendEntriesResponse,
                              mterm           |-> currentTerm[i],
                              msuccess        |-> TRUE,
                              mmatchIndex     |-> m.mprevLogIndex +
                                                    Len(m.mentries),
                              msource         |-> i,
                              mdest           |-> j],
                              m)
                    /\ UNCHANGED <<candidateVars, leaderVars, votedFor, currentTerm, 
                                   auxVars, clientVars>>
       
\* ACTION: HandleAppendEntriesResponse
\* Server i receives an AppendEntries response from server j with
\* m.mterm = currentTerm[i].
HandleAppendEntriesResponse ==
    \E m \in DOMAIN messages :
        /\ ReceivableMessage(m, AppendEntriesResponse, EqualTerm)
        /\ LET i     == m.mdest
               j     == m.msource
           IN
              /\ \/ /\ m.msuccess \* successful
                    /\ nextIndex'  = [nextIndex  EXCEPT ![i][j] = m.mmatchIndex + 1]
                    /\ matchIndex' = [matchIndex EXCEPT ![i][j] = m.mmatchIndex]
                 \/ /\ \lnot m.msuccess \* not successful
                    /\ nextIndex' = [nextIndex EXCEPT ![i][j] =
                                         Max({nextIndex[i][j] - 1, 1})]
                    /\ UNCHANGED <<matchIndex>>
              /\ pendingResponse' = [pendingResponse EXCEPT ![i][j] = FALSE]
              /\ Discard(m)
              /\ UNCHANGED <<serverVars, candidateVars, raftLogVars, auxVars, xraftServerVars, clientVars>>

(*************************************)
(* This is the specification of Fast *)
(*************************************)


\* Leader i sends prepareToFast request to replica j.
\* tell that all servers to change to fast
PrepareToFast ==
    \E i \in Server: 
        /\ state[i] = Leader 
        /\ faststate[i] = Slow 
        /\  LET logOk == \A j \in Server:  \* 日志已经同步到最新
                            nextIndex[j]=matchIndex[j]
                entry == [type |-> PrepareToFastRequest,
                          term  |-> currentTerm[i], 
                          fastTerm |-> fastTerm[i]+ 1]
                newLog == Append(log[i], entry)
            IN  /\ faststate' = [faststate EXCEPT ![i] = FastPrepare]  \* start block the later raft log
                /\ log' = [log EXCEPT ![i] = newLog] \* 将PrepareToFastRequest插入到Leader的Raft log当中
        /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars,auxVars, commitIndex, clientVars, fastTerm, fastlog, committedLog>>
        



\* client Leader i issue a request to add v to the log.
SubmitClientRequest(i, v) ==
    /\ clientApplied[i] < MaxReqs 
    /\ Len(clientLog[i]) = clientApplied[i]  \* 所有旧req被处理完后，生成一个新的req
    /\ LET entry == [entryType |-> WRITE, 
                     findex |-> Len(clientLog[i]) + 1,
                     client |-> i,
                     value |-> v]
           newLog == Append(clientLog[i], entry)
           committedIndex == {index \in 1..Len(clientLogState[i]) : clientLogState[i][index] = COMMITTED}
           committedValue == { clientLog[i][index].value : index \in committedIndex }
       IN  clientLog' = [clientLog EXCEPT ![i] = newLog]
            /\ clientLogResponded' = [clientLogResponded EXCEPT ![i] = Append(clientLogResponded[i],{})]
            /\ fIndex' = [fIndex EXCEPT ![i] = fIndex[i]+1]
            /\ clientLogState' = [clientLogState EXCEPT ![i] = Append(clientLogState[i], UNCOMMITTED)] 
            /\ clientCanSend' = [clientCanSend EXCEPT ![i] = FALSE]
            /\ SendMultipleOnce({[mtype     |->     ClientRequest,
                                                mcommittedValue |-> committedValue,
                                                mfindex        |-> entry.findex,
                                                mrequest       |-> entry,    \* 发送最后一条entry
                                                msource        |-> i, 
                                                mdest          |-> j]: j \in Server \ {i}})
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, raftLogVars,auxVars, clientApplied, clientKonwFTerm, xraftServerVars>>

 

\* Client i sends j an Fast/Slow request containing up to 1 entry.
\* While implementations may want to send more than 1 at a time, this spec uses
\* just 1 because it minimizes atomic regions without loss of generality.
\* The client always tries to make requests in Fast mode, even if the current Raft leader is in Slow Path.
\* ClientSendRequest(i) ==
\*     /\ clientCanSend[i]
\*     /\ clientCanSend' = [clientCanSend EXCEPT ![i] = FALSE]
\*     /\ LET committedIndex == { index \in clientLogState[i]:
\*                                     /\ clientLogState[i][index] = COMMITTED}
\*            committedValue == { clientLog[i][index].value : index \in commitIndex }
\*         IN  /\ SendMultipleOnce({[mtype     |->     ClientRequest,
\*                                         mcommittedValue |-> committedValue,
\*                                         mfindex        |-> clientLog[i][Len(clientLog[i])].findex,
\*                                         mrequest       |-> clientLog[i][Len(clientLog[i])],    \* 发送最后一条entry
\*                                         msource        |-> i, 
\*                                         mdest          |-> j]: j \in Server \ {i}})
\*     /\ UNCHANGED <<serverVars, candidateVars, leaderVars, raftLogVars, clientLog, fIndex, clientLogResponded, clientLogState, xraftServerVars>>

\* server i receive a request from client j
\* server do the gc to update all committed fast log.
UpdateClientCommit(i, j, m) == 
    /\ \A k \in 1..Len(m.mcommittedValue): 
        \E o \in 1..Len(fastlog[i][m.fterm]):   \* 找到目标operation，进行提交
        /\ fastlog[i][m.fterm][o].client = j 
        /\ fastlog[i][m.fterm][o].findex = m.mcommittedValue[k]
        /\ fastlog' = [fastlog EXCEPT ![i][m.fterm][o] = TRUE]
        /\ committedLog' = [committedLog EXCEPT ![i][m.mcommittedValue[k]] = Append(committedLog[i][m.mcommittedValue[k]], j)] 
        /\ PrintVal("server commit ID", [server |-> i, ID |-> j, Value |-> m.mcommittedValue[k]])
        /\ acked' = [v \in Value |-> [
                           c \in Client |-> 
                            IF acked[v][c] = FALSE
                            THEN [Val |-> v, Client |-> c] \in { [Val |-> m.mcommittedValue[k], Client |-> j] }
                            ELSE acked[v][c]
                     ]]


\* Server i receives an FastRequest request from client j with
\* m.mterm <= currentTerm[i]. This just handles m.entries of length 0 or 1, but
\* implementations could safely accept more by treating them the same as
\* multiple independent requests of 1 entry.
HandleClientRequestMsgF(i, j, m) ==
    \/ (/\ state[i] = Leader  \* 如果当前是leader且处于SLOW状态
        /\ (faststate[i] = Slow \/ faststate[i] = Merge) 
        /\ acked[m.mrequest.value][m.mrequest.client] = Nil \* prevent submitting the same value repeatedly
        /\  LET entry == [type  |-> RaftLogEntry,
                    term          |-> currentTerm[i],
                    value         |-> m.mrequest,
                    mfindex         |-> m.mfindex, 
                    mess          |-> m,
                    source        |-> i]    
                    newRaftLog == Append(log[i], entry) \* send the fastlog through raft 
            IN  /\ log' = [log EXCEPT ![i] = newRaftLog]
                /\ acked' = [acked EXCEPT ![m.mrequest.value][m.mrequest.client] = FALSE]
        /\ UNCHANGED <<messages, serverVars, candidateVars,
                   leaderVars, commitIndex, electionCtr, restartCtr, xraftServerVars, clientVars>>
       )
    \/ (/\ faststate[i] = Fast
        /\ acked[m.mrequest.value][m.mrequest.client] = Nil
        /\ acked' = [acked EXCEPT ![m.mrequest.value][m.mrequest.client] = FALSE]
        \* /\ PrintVal("Replica process a Fast Request", [server |-> i, from |-> i])
        /\  LET fterm == fastTerm[i]
                conflict == IF Len(fastlog[i][fterm]) = 0
                    THEN FALSE
                    ELSE \E z \in 1..Len(fastlog[i][fterm]):
                            /\ fastlog[i][fterm][z].client \notin  {committedLog[i][fastlog[i][fterm][z].value][idx] : idx \in 1..Len(committedLog[i][fastlog[i][fterm][z].value]) }  \* 该log没有提交
                            /\ fastlog[i][fterm][z].entryType = WRITE
                            /\ fastlog[i][fterm][z].value = m.mrequest.value 
                exist ==IF Len(fastlog[i]) = 0
                        THEN FALSE
                        ELSE /\ "mrequest" \in DOMAIN m
                             /\ (\/ (\E t \in 1..Len(fastlog[i]):   \* fast log当中没有该项
                                    \E z \in 1..Len(fastlog[i][t]):  
                                    /\ fastlog[i][t][z].client = m.mrequest.client  
                                    /\ fastlog[i][t][z].findex = m.mrequest.findex)
                                \/ (\E z \in 1..Len(log[i]):          \* raft log当中没有该项
                                    /\ log[i][z].type = RaftLogEntry 
                                    /\ log[i][z].client = m.mrequest.client  
                                    /\ fastlog[i][z].findex = m.mrequest.findex)
                                )
                depsIndex == { z \in 1..Len(fastlog[i][fterm]):
                                   /\ fastlog[i][fterm][z].client \notin {committedLog[i][fastlog[i][fterm][z].value][idx] : idx \in 1..Len(committedLog[i][fastlog[i][fterm][z].value]) }
                                   /\ fastlog[i][fterm][z].entryType = WRITE
                                   /\ fastlog[i][fterm][z].value = m.mrequest.value }
                depsEntries == {
                            fastlog[i][fterm][index] : index \in depsIndex
                }
            IN  /\  \/ /\ Cardinality(m.mcommittedValue) > 0
                       /\ UpdateClientCommit(i,j,m)        \* first update all committed fastlog
                    \/ /\ Cardinality(m.mcommittedValue) = 0
                       /\ UNCHANGED << committedLog >>
                /\ ~ exist 
                /\ "mrequest" \in DOMAIN m
                /\ fastlog' = [fastlog EXCEPT ![i][fterm] = Append(fastlog[i][fterm], m.mrequest)]
                /\  \/ (/\ conflict
                       /\  Reply([mtype           |-> ClientResponse,
                                mfterm           |-> fterm,
                                msuccess        |-> FAST_CONFLICT,
                                mfindex         |-> m.mfindex,
                                mraftstate      |-> state[i],
                                mdeps            |-> depsEntries,
                                msource         |-> i,
                                mdest           |-> j],
                                m))
                    \/  /\ ~ conflict 
                        /\ Reply([mtype |-> ClientResponse,
                            mfterm           |-> fterm,
                            msuccess        |-> FAST_SUCCESS,
                            mfindex         |-> m.mfindex,
                            mdeps            |-> depsEntries,
                            mraftstate      |-> state[i],
                            msource         |-> i,
                            mdest           |-> j],
                            m)
                /\ UNCHANGED <<serverVars, candidateVars, leaderVars, raftLogVars, clientVars, faststate, fastTerm, electionCtr, restartCtr>>
        )
           
HandleClientRequestMsg == 
    \E m \in DOMAIN messages :
      /\ Receivable2PCMessage(m, ClientRequest)  
      /\ LET i     == m.mdest
             j     == m.msource
         IN /\ HandleClientRequestMsgF(i, j, m)
       
\* Client i receives an ClientRequest response from server j with
HandleClientResponseMsgF(i, j, m) ==
    \/  /\  m.msuccess = SLOW_SUCCESS 
        /\ PrintVal("get a slow success", [success |-> m.msuccess, mfindex |-> m.mfindex])
        /\  clientLogState[i][m.mfindex] = UNCOMMITTED
        /\  clientLogState' = [clientLogState EXCEPT ![i][m.mfindex] = COMMITTED]
        /\(\/ ( /\ m.fterm > clientKonwFTerm[i]
                /\ clientKonwFTerm' = [clientKonwFTerm EXCEPT ![i]=m.fterm]
              )
           \/ ( /\ m.fterm <= clientKonwFTerm[i]
                /\ UNCHANGED << clientKonwFTerm >>
              ))
        /\  UNCHANGED <<messages, serverVars, candidateVars, leaderVars, raftLogVars, clientLog, fIndex, clientLogResponded, xraftServerVars>>
       
    \/ (/\ m.msuccess # SLOW_SUCCESS 
        /\ m.mfterm >= clientKonwFTerm[i] \* 过滤所有的旧消息
        /\ LET entry == [ 
            from |-> j,
            success |-> m.msuccess,
            mstate  |-> m.mraftstate,
            mfterm   |-> m.mfterm,
            deps   |-> m.mdeps]
            IN clientLogResponded' = [clientLogResponded EXCEPT ![i][m.mfindex] = clientLogResponded[i][m.mfindex] \cup entry]  \* 向clientLogResponded集合当中添加元素（应该确保去重）
        /\      /\ Cardinality(clientLogResponded[i][m.mfindex]) = Cardinality(Server)
                /\ PrintVal("Client receive all fast response", [client |-> i, from |-> j])
                /\ LET ALL_ACCEPT == 
                        \A k \in DOMAIN clientLogResponded[i][m.mfindex] :
                            clientLogResponded[i][m.mfindex][k].success = FAST_SUCCESS
                    ALL_SAME_DEP ==
                        LET 
                            firstEntry == clientLogResponded[i][m.mfindex][0]
                            firstDeps  == firstEntry.deps
                        IN  \A k \in DOMAIN clientLogResponded[i][m.mfindex] :
                            clientLogResponded[i][m.mfindex][k].deps = firstDeps
                    ALL_SAME_FTerm ==
                        LET 
                            firstEntry == clientLogResponded[i][m.mfindex][0]
                            firstTerm == firstEntry.mterm 
                        IN  \A k \in DOMAIN clientLogResponded[i][m.mfindex] :
                            clientLogResponded[i][m.mfindex][k].fastTerm = firstTerm
                    MAX_FTerm == 
                            LET  terms == {e.mfterm : e \in clientLogResponded[i][m.mfindex]}
                            IN Max(terms)
                    IN  \/ /\ MAX_FTerm > clientKonwFTerm[i]
                           /\ clientKonwFTerm' = [clientKonwFTerm EXCEPT ![i]=MAX_FTerm]
                            
                        \/ /\ MAX_FTerm <=  clientKonwFTerm[i]
                           /\ UNCHANGED << clientKonwFTerm >>
                           
                        \/ /\ ALL_ACCEPT
                           /\ ALL_SAME_FTerm 
                           /\ clientLogState' = [clientLogState EXCEPT ![i][m.mfindex] = COMMITTED]
                        \/ /\ ALL_SAME_DEP 
                           /\ ALL_SAME_FTerm
                           /\ clientLogState' = [clientLogState EXCEPT ![i][m.mfindex] = COMMITTED] 
                        \/ /\ ( \lnot ALL_SAME_FTerm \/ (\lnot ALL_ACCEPT /\ \lnot ALL_SAME_DEP ) ) \* 不是同一个fast term 或者没有被接受
                           /\ Send([mtype          |-> ClientAbort, \* abort 当前fast请求
                                        mfterm          |-> MAX_FTerm,
                                        mentries       |-> {}, 
                                        msource        |-> i, 
                                        mdest          |-> j]) 
                           /\  UNCHANGED <<clientLogState>>
                            
                        
                           
        /\ UNCHANGED << messages, serverVars, candidateVars, leaderVars, raftLogVars, clientLog, fIndex,xraftServerVars>>
                
        )

HandleClientResponseMsg == 
    \E m \in DOMAIN messages :
      /\ Receivable2PCMessage(m, ClientResponse)  
      /\ LET i     == m.mdest
             j     == m.msource
         IN /\ HandleClientResponseMsgF(i, j, m)
                                                
\* Server i receive abort request, it needs to change to raft
MergeSync(i, ferm) ==
    /\ state[i] = Leader
    /\ fastTerm[i] = ferm
    /\ faststate[i] = Fast   \* Leader 在fast mode时才可以进行merge
    /\ faststate' = [faststate EXCEPT ![i] = Merge]
    \* /\ Print("Merge sync start",TRUE)
    /\ LET  entries == [type  |-> MergeRequest,
                term          |-> currentTerm[i],
                mentries       |-> fastlog[i][fastTerm[i]],
                fastTerm       |->   fastTerm[i]]    
            newRaftLog == Append(log[i], entries) \* send the fastlog through raft 
       IN  log' = [log EXCEPT ![i] = newRaftLog]
    /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, commitIndex, clientVars, fastTerm, fastlog, committedLog, auxVars>>
    
   
\* ACTION: UpdateTerm
\* Any RPC with a newer term causes the recipient to advance its term first.
UpdateTerm ==
    \E m \in DOMAIN messages :
        /\ m.msource \notin Client 
        /\ m.mdest \notin Client 
        /\ m.mterm > currentTerm[m.mdest]
        /\ currentTerm'    = [currentTerm EXCEPT ![m.mdest] = m.mterm]
        /\ state'          = [state       EXCEPT ![m.mdest] = Follower]
        /\ votedFor'       = [votedFor    EXCEPT ![m.mdest] = Nil]
           \* messages is unchanged so m can be processed further.
        /\ UNCHANGED <<messages, candidateVars, leaderVars, raftLogVars, auxVars, clientVars, xraftServerVars>>  


\* Responses with stale terms are ignored.
DropStaleResponse(i, j, m) ==
    /\ m.mterm < currentTerm[i]
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, raftLogVars, clientVars, xraftServerVars>>

          
\* End of message handlers.
----
\* Network state transitions

\* The network duplicates a message
DuplicateMessage(m) ==
     /\ Duplicate(m)
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, raftLogVars, clientVars, xraftServerVars>>

\* The network drops a message
DropMessage(m) ==
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, raftLogVars, clientVars, xraftServerVars>>

----
\* Defines how the variables may transition.
Next == 
     \* xRaft
        \/ \E i \in Client, v \in Value : SubmitClientRequest(i, v) \* 生成一个新的 request
        \/ \E i \in Server : MergeSync(i, fastTerm[i])  \* Merge 发生
        \/ PrepareToFast \* 进入Fast 模式

        \/ \E i \in Server : Restart(i)
        \/ \E i \in Server : RequestVote(i)
        \/ \E i \in Server : BecomeLeader(i)
        \/ \E i \in Server : AdvanceCommitIndex(i)
        \/ \E i,j \in Server : AppendEntries(i, j)
        \/ UpdateTerm
        \/ HandleRequestVoteRequest
        \/ HandleRequestVoteResponse
        \/ RejectAppendEntriesRequest
        \/ AcceptAppendEntriesRequest
        \/ HandleAppendEntriesResponse
        \/ HandleClientRequestMsg
        \/ HandleClientResponseMsg
        

       


----
\* Define initial values for all variables

InitServerVars == /\ currentTerm = [i \in Server |-> 1]
                  /\ state       = [i \in Server |-> Follower]
                  /\ votedFor    = [i \in Server |-> Nil]
InitCandidateVars == votesGranted   = [i \in Server |-> {}]
\* The values nextIndex[i][i] and matchIndex[i][i] are never read, since the
\* leader does not send itself messages. It's still easier to include these
\* in the functions.
InitLeaderVars == /\ nextIndex  = [i \in Server |-> [j \in Server |-> 1]]
                  /\ matchIndex = [i \in Server |-> [j \in Server |-> 0]]
InitLogVars == /\ log             = [i \in Server |-> << >>]
               /\ commitIndex     = [i \in Server |-> 0]
               /\ pendingResponse = [i \in Server |-> [j \in Server |-> FALSE]]
InitAuxVars == /\ electionCtr = 0
               /\ restartCtr = 0
               /\ acked = [v \in Value |-> [c \in Client |-> Nil]]

InitxRaft ==
    /\ fastTerm = [i \in Server |-> 0]
    /\ committedLog = [i \in Server |-> [v \in Value |-> << >>]]
    /\ faststate = [i \in Server |-> Slow]
    /\ fastlog = [i \in Server |-> << >>]
    
InitClientLogVars ==
    /\ fIndex  = [i \in Client |-> 1]
    /\ clientLog = [i \in Client |-> << >>]
    /\ clientLogResponded = [i \in Client |-> << >>]
    /\ clientKonwFTerm = [i \in Client |-> 1]
    /\ clientLogState = [i \in Client |-> << >>]
    /\ clientApplied = [i \in Client |-> 0]
    /\ clientCanSend = [i \in Client |-> TRUE]



Init == /\ messages = [m \in {} |-> 0]
        /\ InitServerVars
        /\ InitCandidateVars
        /\ InitLeaderVars
        /\ InitLogVars
        /\ InitAuxVars
        /\ InitClientLogVars
        /\ InitxRaft



\* The specification must start with the initial state and transition according
\* to Next.
NoStuttering ==
    WF_vars(Next)

Spec == Init /\ [][Next]_vars

LivenessSpec == Init /\ [][Next]_vars /\ NoStuttering





----
\* LIVENESS   -------------------------

\* ValuesNotStuck -----------------
\* A client value will either get committed and be
\* fully replicated or it will be truncated and
\* not be found on any server log.
\* Note that due to the number of elections being limited,
\* the last possible election could fail and prevent
\* progress, so this liveness formula only apples in cases
\* a behaviour does not end with all elections used up
\* and no elected leader.
ValueInServerLog(i, v) ==
    \E index \in DOMAIN log[i] :
        log[i][index].value = v

ValueAllOrNothing(v) ==
    IF /\ electionCtr = MaxElections
       /\ ~\E i \in Server : state[i] = Leader
    THEN TRUE
    ELSE \/ \A i \in Server : ValueInServerLog(i, v)
         \/ ~\E i \in Server : ValueInServerLog(i, v)

ValuesNotStuck ==
    \A v \in Value : []<>ValueAllOrNothing(v)

\* INVARIANTS -------------------------

MinCommitIndex(s1, s2) ==
    IF commitIndex[s1] < commitIndex[s2]
    THEN commitIndex[s1]
    ELSE commitIndex[s2]

\* INV: NoLogDivergence
\* The log index is consistent across all servers (on those
\* servers whose commitIndex is equal or higher than the index).
NoLogDivergence ==
    \A s1, s2 \in Server :
        IF s1 = s2
        THEN TRUE
        ELSE
            LET lowest_common_ci == MinCommitIndex(s1, s2)
            IN IF lowest_common_ci > 0
               THEN \A index \in 1..lowest_common_ci : log[s1][index] = log[s2][index]
               ELSE TRUE

\* INV: Used in debugging
TestInv ==
    TRUE

\* INV: LeaderHasAllAckedValues
\* A non-stale leader cannot be missing an acknowledged value
LeaderHasAllAckedValues ==
    \* for every acknowledged value
    \A v \in Value :
    \A c \in Client :
        IF acked[v][c] = TRUE
        THEN
            \* there does not exist a server that
            ~\E i \in Server :
                \* is a leader
                /\ state[i] = Leader
                \* and which is the newest leader (aka not stale)
                /\ ~\E l \in Server : 
                    /\ l # i
                    /\ currentTerm[l] > currentTerm[i]
                \* and that is missing the value
                /\ ~\E index \in DOMAIN log[i] :
                    log[i][index].value = v
        ELSE TRUE

\* INV: CommittedEntriesReachMajority
\* There cannot be a committed entry that is not at majority quorum
\* Don't use this invariant when allowing data loss on a server.
CommittedEntriesReachMajority ==
    IF \E i \in Server : state[i] = Leader /\ commitIndex[i] > 0
    THEN \E i \in Server :
           /\ state[i] = Leader
           /\ commitIndex[i] > 0
           /\ \E quorum \in SUBSET Server :
               /\ Cardinality(quorum) = (Cardinality(Server) \div 2) + 1
               /\ i \in quorum
               /\ \A j \in quorum :
                   /\ Len(log[j]) >= commitIndex[i]
                   /\ log[j][commitIndex[i]] = log[i][commitIndex[i]]
    ELSE TRUE              

=============================================================================

\* Modification History
\* Last modified Sun Jul 28 20:44:35 CST 2024 by xrfgo
\* Created Sun Jul 28 20:43:58 CST 2024 by xrfgo

