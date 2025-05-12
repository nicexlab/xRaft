package xraft

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

import (
	"fmt"
	"log"

	xkv "github/Fischer0522/xraft/kv"

	"github/Fischer0522/xraft/xraft/conn"
	xlog "github/Fischer0522/xraft/xraft/log"
	"github/Fischer0522/xraft/xraft/pb"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.uber.org/zap"
)

// a key-value store backed by raft
type StateMachine struct {
	id        int
	raftState raft.StateType
	proposeC  chan<- string // channel for proposing updates
	// mu       sync.RWMutex

	kvStore     xkv.KVStore
	snapshotter *snap.Snapshotter

	Done chan struct{}

	proxy *conn.Participants_2PC // client proxy

	logger *zap.SugaredLogger

	fterm uint32

	triggerFastC chan struct{}
}

func NewStateMachine_grpc_with_fast_batch(serverAddr string, id int, snapshotter *snap.Snapshotter, proposeC chan<- string, statC <-chan raft.StateType, commitC <-chan *commit, errorC <-chan error, grpcAddrs []string) *StateMachine {
	s := &StateMachine{

		id:           id,
		raftState:    raft.StateFollower,
		proposeC:     proposeC,
		snapshotter:  snapshotter,
		triggerFastC: make(chan struct{}),
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
	s.logger = xlog.InitLogger().Named(fmt.Sprintf("xRaft-%v:", id))

	kv, err := xkv.NewMemStore()
	if err != nil {
		s.logger.Fatal(err)
	}
	s.kvStore = kv
	proxy, SlowPathC, PrepareKeyToFastC := conn.NewParticipants_2PC(id, kv, 3, grpcAddrs)
	go conn.Startgrpc(proxy, serverAddr) // 启动rpc服务
	s.proxy = proxy
	go s.server(commitC, statC, errorC, SlowPathC, PrepareKeyToFastC)
	return s
}

func NewStateMachine_grpc(serverAddr string, id int, snapshotter *snap.Snapshotter, proposeC chan<- string, statC <-chan raft.StateType, commitC <-chan *commit, errorC <-chan error) *StateMachine {
	s := &StateMachine{

		id:           id,
		raftState:    raft.StateFollower,
		proposeC:     proposeC,
		snapshotter:  snapshotter,
		triggerFastC: make(chan struct{}),
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
	s.logger = xlog.InitLogger().Named(fmt.Sprintf("xRaft-%v:", id))

	kv, err := xkv.NewMemStore()
	if err != nil {
		s.logger.Fatal(err)
	}
	s.kvStore = kv
	proxy, SlowPathC, PrepareKeyToFastC := conn.NewParticipants_2PC(id, kv, 3, nil)
	go conn.Startgrpc(proxy, serverAddr) // 启动rpc服务
	s.proxy = proxy
	go s.server(commitC, statC, errorC, SlowPathC, PrepareKeyToFastC)
	return s
}

func NewStateMachine(id int, snapshotter *snap.Snapshotter, proposeC chan<- string, statC <-chan raft.StateType, commitC <-chan *commit, errorC <-chan error) *StateMachine {
	s := &StateMachine{
		id:           id,
		raftState:    raft.StateFollower,
		proposeC:     proposeC,
		snapshotter:  snapshotter,
		triggerFastC: make(chan struct{}),
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
	s.logger = xlog.InitLogger().Named(fmt.Sprintf("xRaft-%v:", id))

	kv, err := xkv.NewMemStore()
	if err != nil {
		s.logger.Fatal(err)
	}
	s.kvStore = kv
	proxy, mergeC, SlowPathC := conn.NewParticipants_2PC(id, kv, 3, nil)
	s.proxy = proxy
	go s.server(commitC, statC, errorC, mergeC, SlowPathC)
	return s
}

func (s *StateMachine) Lookup(key string) (string, xkv.KvOpStatus) {
	// s.mu.RLock()
	// defer s.mu.RUnlock()
	// v, ok := s.kvStore[key]
	v, err := s.kvStore.Get(key)
	var stat xkv.KvOpStatus
	if err == nil {
		stat = xkv.SUCCEED
	} else {
		stat = xkv.KEY_NOT_EXIST
	}
	return v, stat
}

func (s *StateMachine) IsLeader() bool {
	return s.raftState == raft.StateLeader
}

func (s *StateMachine) server(commitC <-chan *commit, statC <-chan raft.StateType, errorC <-chan error, mergeC <-chan *conn.MergeInfo, SlowPathC <-chan *pb.Request) {
	s.logger.Infof("start server.")
	for {
		select {
		case commit, ok := <-commitC:
			if !ok {
				s.proxy.Close()
				if err, ok := <-errorC; ok {
					log.Fatal(err)
				}
				s.kvStore.Destroy()
				return
			}
			log.Printf("****************get a commit******************")
			// 相比于MIT 6.824 此处的snapshot为真正持久化的，因此无需通过channel传输
			// 因此在chan当中只需要发送一个nil用于通知即可
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
				break
			}

			for _, data := range commit.data {
				s.dispatchCommand(data)
			}
			close(commit.applyDoneC)

		case stat := <-statC:
			s.raftState = stat
			s.logger.Debugf("become %v", s.raftState.String())
			if s.IsLeader() {
				s.proxy.BecomeLeader()
			} else {
				s.proxy.BecomeNone()
			}
		// 使用 <- s.proxy.XXXC时 需要小心死锁
		case slow_cmd := <-SlowPathC:
			if s.IsLeader() { // follower 不需要处理slow command的发送
				// 处理slow_cmd
				s.logger.Debugf("get a slow command %v", slow_cmd)
				// b, _ := slow_cmd.Encode()
				s.proposeC <- string(EncodeRequestWithSlow(slow_cmd))
			}

		case m := <-mergeC:
			if s.IsLeader() {
				s.proposeC <- string(EncodeMergInfo(m)) // 发送给共识
			}
		case <-s.triggerFastC: // 准备将这个key放入快速路径上
			if s.IsLeader() {
				//  leader首先需要阻塞其2pc向xraft转发该key至慢速路径
				// 待完成prepare fast命令之后leader 调用SetKeyToFast来告知2pc可以服务该key
				// todo: 选择一个合适的时机来完成这个转换
				s.logger.Info("leader prepare to fast")
				s.proxy.SetBlockedToFast()
				slowl := len(SlowPathC)
				last_cmd := make([]*pb.Request, slowl)

				for i := 0; i < slowl; i++ { //清空slowC
					// last_cmd := <-SlowPathC
					// s.proposeC <- string(EncodeRequestWithSlow(last_cmd))
					last_cmd[i] = <-SlowPathC
				}

				s.proposeC <- string(EncodeBatchedSlowCmds(last_cmd))

				preFastCmd := PrepareFastCommand{FastTerm: s.fterm + 1}
				s.proposeC <- string(preFastCmd.Encode())
			}
		}
	}
}

func (s *StateMachine) executePrepareFastCommand(cmd PrepareFastCommand) {
	s.proxy.SetFast(cmd.FastTerm)
	s.fterm = cmd.FastTerm
}

// SetSlow repair the un-ordered conflict commands
func (s *StateMachine) executeSetSlowCommand(cmd *conn.MergeInfo) {
	s.logger.Debugf("execute set slow commands %v", cmd)
	s.proxy.SetToSlow(cmd)
	go func() {
		s.triggerFastC <- struct{}{}
	}()
}

func (s *StateMachine) dispatchCommand(data string) {

	// TODO: do we need another gorountine to handle it?
	// decode uint16 to get type
	buf := []byte(data)
	// TODO remove this magic number
	commandType := DecodeCommandType(buf[0:2])
	buf = buf[2:]
	switch commandType {
	case pb.SlowCmd:
		s.logger.Debugf("Get a slow commmand")

		slowCommand, _ := DecodeRequest(buf)
		s.proxy.ExecuteSlowCommand(slowCommand)
	case pb.PrepareFastCmd: // 将key放入快速路径上
		s.logger.Debugf("Get a prepare fast commmand")
		prepareCommand := DecodePrepareFastCommand(buf)
		s.executePrepareFastCommand(prepareCommand)
	case pb.SetSlowCmd: // 将key放入慢速路径上
		s.logger.Debugf("Get a set slow commmand")
		setSlowCmd := DecodeSetSlowCommand(buf)
		s.executeSetSlowCommand(setSlowCmd)
	case pb.BatchedSCmd:
		s.logger.Debugf("Get a batchedCmd")
		cmds := DecodeBatchedSlowCmds(buf)
		s.proxy.ExecuteBatchedSlowCommand(cmds)
	case pb.ClientCmd:
		s.logger.Panicf("ClientCmd not used")
	case pb.OrderCmd:
		s.logger.Panicf("Ordercmd not used")
	default:
		s.logger.Panicf("unknown command type")
	}
}

func (s *StateMachine) getSnapshot() ([]byte, error) {
	// s.mu.RLock()
	// defer s.mu.RUnlock()
	return nil, nil
}

func (s *StateMachine) loadSnapshot() (*raftpb.Snapshot, error) {
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
	// store := xkv.InitMem_KvStore()
	// store.RecoverFromSnapshot(snapshot)
	// // s.mu.Lock()
	// // defer s.mu.Unlock()
	// s.kvStore = store
	return nil
}
