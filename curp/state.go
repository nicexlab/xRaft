// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package curp

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"

	"github/Fischer0522/xraft/curp/command"
	"github/Fischer0522/xraft/curp/curp_proto"
	"github/Fischer0522/xraft/curp/witness"
	xkv "github/Fischer0522/xraft/kv"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"

	"github/Fischer0522/xraft/curp/trace"
)

// a key-value store backed by raft
type StateMachine struct {
	NodeId   int
	proposeC chan<- string // channel for proposing updates
	mu       sync.RWMutex
	// kvStore      map[string]string // current committed key-value pairs
	KVStore      xkv.KVStore
	commandBoard *command.CommandBoard
	snapshotter  *snap.Snapshotter
	raftState    raft.StateType
	fastPath     chan *curp_proto.CurpClientCommand
	slowPath     chan *curp_proto.CurpClientCommand
	// both fast and slow path will execute commands,this set is used to avoid executing the same command twice
	commandSet   map[command.ProposeId]struct{}
	witness      witness.Witness
	stateChangeC chan raft.StateType
}

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
}

func newGrpcState(rpcAddr string, nodeId int, snapshotter *snap.Snapshotter, proposeC chan<- string, stateC chan raft.StateType, commitC <-chan *commit, errorC <-chan error) *StateMachine {
	kv, _ := xkv.NewMemStore()
	s := &StateMachine{
		NodeId:   nodeId,
		proposeC: proposeC,
		// kvStore:      make(map[string]string),
		KVStore:      kv,
		snapshotter:  snapshotter,
		raftState:    raft.StateFollower,
		fastPath:     make(chan *curp_proto.CurpClientCommand, 1000),
		slowPath:     make(chan *curp_proto.CurpClientCommand, 1000),
		commandSet:   make(map[command.ProposeId]struct{}),
		commandBoard: command.NewBoard(),
		witness:      witness.NewWitness(),
		stateChangeC: stateC,
	}
	snapshot, err := s.loadSnapshot()
	if err != nil {
		log.Panic(err)
	}
	if snapshot != nil {
		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			log.Panic(err)
		}
	}
	go Startgrpc(s, rpcAddr)
	trace.Trace(trace.Info, nodeId, "init curp state machine")
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	go s.cmd_worker()
	go s.listenRaftState()
	return s
}

func newState(nodeId int, snapshotter *snap.Snapshotter, proposeC chan<- string, stateC chan raft.StateType, commitC <-chan *commit, errorC <-chan error) *StateMachine {
	kv, _ := xkv.NewBoltStore(fmt.Sprintf("curp-%v", nodeId), "test")
	s := &StateMachine{
		NodeId:   nodeId,
		proposeC: proposeC,
		// kvStore:      make(map[string]string),
		KVStore:      kv,
		snapshotter:  snapshotter,
		raftState:    raft.StateFollower,
		fastPath:     make(chan *curp_proto.CurpClientCommand, 1000),
		slowPath:     make(chan *curp_proto.CurpClientCommand, 1000),
		commandSet:   make(map[command.ProposeId]struct{}),
		commandBoard: command.NewBoard(),
		witness:      witness.NewWitness(),
		stateChangeC: stateC,
	}
	snapshot, err := s.loadSnapshot()
	if err != nil {
		log.Panic(err)
	}
	if snapshot != nil {
		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			log.Panic(err)
		}
	}
	trace.Trace(trace.Info, nodeId, "init curp state machine")
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	go s.cmd_worker()
	go s.listenRaftState()
	return s
}

func (s *StateMachine) IsLeader() bool {
	return s.raftState == raft.StateLeader
}

func (s *StateMachine) Lookup(cmd *curp_proto.CurpClientCommand) *curp_proto.CurpReply {
	result := s.Propose(cmd)
	return result
}

func (s *StateMachine) Propose(cmd *curp_proto.CurpClientCommand) *curp_proto.CurpReply {

	isConflict := s.witness.InsertIfNotConflict(cmd)
	s.mu.Lock()
	if s.raftState == raft.StateLeader {
		trace.Trace(trace.Leader, s.NodeId, "propose command %s,type: %s,conflict: %v", cmd.Key, command.OpFmt[cmd.Op], isConflict)
	} else {
		trace.Trace(trace.Follower, s.NodeId, "propose command %s,type: %s,conflict: %v", cmd.Key, command.OpFmt[cmd.Op], isConflict)
	}
	if s.raftState != raft.StateLeader {
		if isConflict {
			// TODO: return and report conflict
			s.mu.Unlock()
			reply := &curp_proto.CurpReply{
				Content:    "",
				StatusCode: curp_proto.CONFLICT,
			}
			return reply
		} else {
			s.mu.Unlock()
			reply := &curp_proto.CurpReply{
				Content:    "",
				StatusCode: curp_proto.ACCEPTED,
			}
			return reply
		}
	}
	buf := cmd.Encode()
	go func() {
		s.fastPath <- cmd
	}()
	if s.raftState == raft.StateLeader {
		// command will update the state machine or get command is conflict
		trace.Trace(trace.Leader, s.NodeId, "send propose[clientId:%d seqId: %d] msg to raft node", cmd.ClientId, cmd.SeqId)
		go func() {
			s.proposeC <- buf
		}()

	}
	s.mu.Unlock()
	proposeId := command.ProposeId{
		ClientId: cmd.ClientId,
		SeqId:    cmd.SeqId,
	}
	result := s.commandBoard.WaitForEr(proposeId)
	reply := &curp_proto.CurpReply{
		Content: result,
	}
	if isConflict {
		// TODO return and report conflict
		reply.StatusCode = curp_proto.CONFLICT
	} else {
		reply.StatusCode = curp_proto.ACCEPTED
	}
	return reply
}

// TODO: wait id to be committed
func (s *StateMachine) WaitSynced(id command.ProposeId) *curp_proto.CurpReply {
	// only send wait synced message to leader
	trace.Trace(trace.Leader, s.NodeId, "got wait synced message: %v", id)
	result := s.commandBoard.WaitForAsr(id)
	reply := &curp_proto.CurpReply{
		Content:    result,
		StatusCode: curp_proto.ACCEPTED,
	}
	trace.Trace(trace.Leader, s.NodeId, "wait synced finished,id: %v,result: %v", id, result)
	return reply
}

func (s *StateMachine) listenRaftState() {

	for state := range s.stateChangeC {
		s.mu.Lock()
		trace.Trace(trace.Vote, s.NodeId, "raft state update: before: %s,current: %s", stmap[s.raftState], stmap[state])
		s.raftState = state
		s.mu.Unlock()
	}
}

func (s *StateMachine) cmd_worker() {
	for {
		select {
		case cmd := <-s.fastPath:
			trace.Trace(trace.Fast, s.NodeId, "got cmd from fast path %v", cmd)
			proposeId := command.ProposeId{
				ClientId: cmd.ClientId,
				SeqId:    cmd.SeqId,
			}
			if _, ok := s.commandSet[proposeId]; ok {
				// command executed before,nothing to do
				trace.Trace(trace.Fast, s.NodeId, "cmd already executed %v", cmd)
			} else {
				s.commandSet[proposeId] = struct{}{}
				result := s.executeSync(cmd)
				trace.Trace(trace.Fast, s.NodeId, "cmd %v executed,result is %v", cmd, result)
				s.commandBoard.InsertEr(proposeId, result)
			}

		case cmd := <-s.slowPath:
			s.mu.Lock()
			isLeader := s.raftState == raft.StateLeader
			s.mu.Unlock()
			trace.Trace(trace.Slow, s.NodeId, "got cmd from slow path %v", cmd)
			if _, ok := s.commandSet[cmd.ProposeId()]; ok && isLeader {
				// command executed before,nothing to do
				trace.Trace(trace.Slow, s.NodeId, "cmd already executed %v", cmd)
			} else {
				s.commandSet[cmd.ProposeId()] = struct{}{}
				s.executeAsync(cmd)
			}
			trace.Trace(trace.Slow, s.NodeId, "WAIT Notify result in slow path,proposeId:[clientId: %d,seqId: %d]", cmd.ClientId, cmd.SeqId)
			s.commandBoard.NotifyAsr(cmd.ProposeId())
			trace.Trace(trace.Slow, s.NodeId, "Notify result in slow path,proposeId:[clientId: %d,seqId: %d]", cmd.ClientId, cmd.SeqId)
			// when command is committed, it can be remove from witness and command set safely
			s.removeRecord(cmd)
		}
	}
}

func (s *StateMachine) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		if commit == nil {
			// signaled to load snapshot
			snapshot, err := s.loadSnapshot()
			if err != nil {
				log.Panic(err)
			}
			if snapshot != nil {
				log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
					log.Panic(err)
				}
			}
			continue
		}

		for _, data := range commit.data {
			var cmd curp_proto.CurpClientCommand
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&cmd); err != nil {
				log.Fatalf("raftexample: could not decode message (%v)", err)
			}
			s.slowPath <- &cmd

		}
		close(commit.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

// TODO: refactor it later
// we don't need to lock this function,because it's the only one which modify KVStore
// same as executeAsync
func (s *StateMachine) executeSync(cmd *curp_proto.CurpClientCommand) string {
	if cmd.Op == command.PUT {
		// s.kvStore[cmd.Key] = cmd.Value
		s.KVStore.Put(cmd.Key, cmd.Value)
	} else if cmd.Op == command.DELETE {
		// delete(s.kvStore, cmd.Key)
		s.KVStore.Delete(cmd.Key)
	} else if cmd.Op == command.GET {
		result, err := s.KVStore.Get(cmd.Key)
		// result, ok := s.kvStore[cmd.Key]
		if err != nil {
			return "NOT FOUND IN STATE"

		} else {
			return result
		}
	}
	return ""
}

func (s *StateMachine) executeAsync(cmd *curp_proto.CurpClientCommand) string {
	if cmd.Op == command.PUT {
		// s.kvStore[cmd.Key] = cmd.Value
		s.KVStore.Put(cmd.Key, cmd.Value)
	} else if cmd.Op == command.DELETE {
		// delete(s.kvStore, cmd.Key)
		s.KVStore.Delete(cmd.Key)
	} else if cmd.Op == command.GET {
		// return s.kvStore[cmd.Key]
		result, _ := s.KVStore.Get(cmd.Key)
		return result
	}
	return ""
}

func (s *StateMachine) removeRecord(cmd *curp_proto.CurpClientCommand) {
	// when the command is executed in slow path it means that it has been replicated to the more than 1/2 followers
	// so we can remove it from witness safely
	s.witness.Remove(cmd)

	// since we only support idempotent command like set x = 5 (set x = x + 5 or append command is non-idempotent),
	// we don't need to worry about that some commands will be executed twice
	// so we can remove it from commandSet safely
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.commandSet, cmd.ProposeId())
}

func (s *StateMachine) getSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// return json.Marshal(s.kvStore)
	return nil, nil
}

func (s *StateMachine) loadSnapshot() (*raftpb.Snapshot, error) {
	if s.snapshotter == nil {
		return nil, nil
	}
	snapshot, err := s.snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *StateMachine) recoverFromSnapshot(snapshot []byte) error {
	// var store map[string]string
	// if err := json.Unmarshal(snapshot, &store); err != nil {
	// 	return err
	// }
	s.mu.Lock()
	defer s.mu.Unlock()
	// s.kvStore = store
	return nil
}
