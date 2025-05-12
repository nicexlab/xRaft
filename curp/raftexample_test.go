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
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github/Fischer0522/xraft/curp/command"
	"github/Fischer0522/xraft/curp/curp_proto"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func getSnapshotFn() (func() ([]byte, error), <-chan struct{}) {
	snapshotTriggeredC := make(chan struct{})
	return func() ([]byte, error) {
		snapshotTriggeredC <- struct{}{}
		return nil, nil
	}, snapshotTriggeredC
}

type cluster struct {
	peers              []string
	commitC            []<-chan *commit
	errorC             []<-chan error
	proposeC           []chan string
	stateC             []chan raft.StateType
	confChangeC        []chan raftpb.ConfChange
	snapshotTriggeredC []<-chan struct{}
	state              []*StateMachine
}

// newCluster creates a cluster of n nodes
func newCluster(n int) *cluster {
	peers := make([]string, n)
	for i := range peers {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", 11000+i)
	}

	clus := &cluster{
		peers:              peers,
		commitC:            make([]<-chan *commit, len(peers)),
		errorC:             make([]<-chan error, len(peers)),
		proposeC:           make([]chan string, len(peers)),
		stateC:             make([]chan raft.StateType, len(peers)),
		confChangeC:        make([]chan raftpb.ConfChange, len(peers)),
		snapshotTriggeredC: make([]<-chan struct{}, len(peers)),
		state:              make([]*StateMachine, len(peers)),
	}

	for i := range clus.peers {
		os.RemoveAll(fmt.Sprintf("raftexample-%d", i+1))
		os.RemoveAll(fmt.Sprintf("raftexample-%d-snap", i+1))
		proposeC := make(chan string, 1)
		stateC := make(chan raft.StateType, 1)

		clus.proposeC[i] = proposeC
		clus.stateC[i] = stateC
		clus.confChangeC[i] = make(chan raftpb.ConfChange, 1)
		fn, snapshotTriggeredC := getSnapshotFn()
		clus.snapshotTriggeredC[i] = snapshotTriggeredC
		commitC, errorC, snapReady := newRaftNode(i+1, clus.peers, false, fn, clus.proposeC[i], clus.stateC[i], clus.confChangeC[i])
		clus.commitC[i] = commitC
		clus.errorC[i] = errorC
		clus.state[i] = newState(i+1, <-snapReady, proposeC, stateC, clus.commitC[i], clus.errorC[i])
	}

	return clus
}
func (clus *cluster) CheckLeader() (int, bool) {
	leader_id := -1

	for i := 0; i < len(clus.peers); i++ {
		if clus.state[i].raftState == raft.StateLeader {
			leader_id = i
			break
		}
	}
	if leader_id == -1 {
		return leader_id, false
	} else {
		return leader_id, true
	}
}

// Close closes all cluster nodes and returns an error if any failed.
func (clus *cluster) Close() (err error) {
	for i := range clus.peers {
		go func(i int) {
			for range clus.commitC[i] {
				// drain pending commits
			}
		}(i)
		close(clus.proposeC[i])
		// wait for channel to close
		if erri := <-clus.errorC[i]; erri != nil {
			err = erri
		}
		// clean intermediates
		os.RemoveAll(fmt.Sprintf("raftexample-%d", i+1))
		os.RemoveAll(fmt.Sprintf("raftexample-%d-snap", i+1))
	}
	return err
}

func (clus *cluster) closeNoErrors(t *testing.T) {
	t.Log("closing cluster...")
	if err := clus.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("closing cluster [done]")
}

// TestProposeOnCommit starts three nodes and feeds commits back into the proposal
// channel. The intent is to ensure blocking on a proposal won't block raft progress.
func TestProposeOnCommit(t *testing.T) {
	clus := newCluster(3)
	defer clus.closeNoErrors(t)

	donec := make(chan struct{})
	for i := range clus.peers {
		// feedback for "n" committed entries, then update donec
		go func(pC chan<- string, cC <-chan *commit, eC <-chan error) {
			for n := 0; n < 1; n++ {
				c, ok := <-cC
				if !ok {
					pC = nil
				}
				select {
				case pC <- c.data[0]:
					continue
				case err := <-eC:
					t.Errorf("eC message (%v)", err)
				}
			}
			donec <- struct{}{}
			for range cC {
				// acknowledge the commits from other nodes so
				// raft continues to make progress
			}
		}(clus.proposeC[i], clus.commitC[i], clus.errorC[i])

		// one message feedback per node
		go func(i int) {
			cmd := clientCmd(uint64(i), uint64(i), command.PUT, "foo", "bar")
			clus.proposeC[i] <- cmd.Encode()
		}(i)
	}

	for range clus.peers {
		<-donec
	}
}

// TestCloseProposerBeforeReplay tests closing the producer before raft starts.
func TestCloseProposerBeforeReplay(t *testing.T) {
	clus := newCluster(1)
	// close before replay so raft never starts
	defer clus.closeNoErrors(t)
}

// TestCloseProposerInflight tests closing the producer while
// committed messages are being published to the client.
func TestCloseProposerInflight(t *testing.T) {
	clus := newCluster(1)
	defer clus.closeNoErrors(t)

	// some inflight ops
	go func() {
		clus.proposeC[0] <- "foo"
		clus.proposeC[0] <- "bar"
	}()

	// wait for one message
	if c, ok := <-clus.commitC[0]; !ok || c.data[0] != "foo" {
		t.Fatalf("Commit failed")
	}
}

func TestPutAndGetKeyValue(t *testing.T) {
	clusters := []string{"http://127.0.0.1:9021"}

	proposeC := make(chan string)
	stateC := make(chan raft.StateType)
	defer close(proposeC)

	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	var kvs *StateMachine
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady := newRaftNode(1, clusters, false, getSnapshot, proposeC, stateC, confChangeC)

	kvs = newState(1, <-snapshotterReady, proposeC, stateC, commitC, errorC)

	// wait server started
	<-time.After(time.Second * 3)

	wantKey, wantValue := "key", "test-value"
	proposeId := command.ProposeId{
		ClientId: 1,
		SeqId:    1,
	}

	cmd := &curp_proto.CurpClientCommand{
		Op:       command.PUT,
		Key:      wantKey,
		Value:    wantValue,
		ClientId: proposeId.ClientId,
		SeqId:    proposeId.SeqId,
	}

	reply := kvs.Propose(cmd)
	if reply.StatusCode != curp_proto.ACCEPTED {
		log.Fatalf("propose failed")
	}
}

// TestAddNewNode tests adding new node to the existing cluster.
func TestAddNewNode(t *testing.T) {
	clus := newCluster(3)
	defer clus.closeNoErrors(t)

	os.RemoveAll("raftexample-4")
	os.RemoveAll("raftexample-4-snap")
	defer func() {
		os.RemoveAll("raftexample-4")
		os.RemoveAll("raftexample-4-snap")
	}()

	newNodeURL := "http://127.0.0.1:10004"
	clus.confChangeC[0] <- raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  4,
		Context: []byte(newNodeURL),
	}

	proposeC := make(chan string)
	stateC := make(chan raft.StateType)
	defer close(proposeC)

	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	newRaftNode(4, append(clus.peers, newNodeURL), true, nil, proposeC, stateC, confChangeC)

	go func() {
		proposeC <- "foo"
	}()

	if c, ok := <-clus.commitC[0]; !ok || c.data[0] != "foo" {
		t.Fatalf("Commit failed")
	}
}

func TestSnapshot(t *testing.T) {
	prevDefaultSnapshotCount := defaultSnapshotCount
	prevSnapshotCatchUpEntriesN := snapshotCatchUpEntriesN
	defaultSnapshotCount = 4
	snapshotCatchUpEntriesN = 4
	defer func() {
		defaultSnapshotCount = prevDefaultSnapshotCount
		snapshotCatchUpEntriesN = prevSnapshotCatchUpEntriesN
	}()

	clus := newCluster(3)
	defer clus.closeNoErrors(t)

	go func() {
		clus.proposeC[0] <- "foo"
	}()

	c := <-clus.commitC[0]

	select {
	case <-clus.snapshotTriggeredC[0]:
		t.Fatalf("snapshot triggered before applying done")
	default:
	}
	close(c.applyDoneC)
	<-clus.snapshotTriggeredC[0]
}

func TestSpeed(t *testing.T) {
	clus := newCluster(3)
	defer clus.closeNoErrors(t)

	round := 2000
	received := 10

	go func() {
		for i := 0; i < round; i++ {
			cmd := clientCmd(1, uint64(i), command.PUT, "foo", "bar")
			clus.proposeC[0] <- cmd.Encode()
		}
	}()

	for data := range clus.commitC[0] {
		batchSize := len(data.data)
		received += batchSize
		fmt.Printf("current received %d", received)
		if received > round {
			break
		}
	}

	// donec := make(chan struct{})
	// for i := range clus.peers {
	// 	// feedback for "n" committed entries, then update donec
	// 	go func(pC chan<- string, cC <-chan *commit, eC <-chan error) {
	// 		for n := 0; n < 1; n++ {
	// 			c, ok := <-cC
	// 			if !ok {
	// 				pC = nil
	// 			}
	// 			select {
	// 			case pC <- c.data[0]:
	// 				continue
	// 			case err := <-eC:
	// 				t.Errorf("eC message (%v)", err)
	// 			}
	// 		}
	// 		donec <- struct{}{}
	// 		for range cC {
	// 			// acknowledge the commits from other nodes so
	// 			// raft continues to make progress
	// 		}
	// 	}(clus.proposeC[i], clus.commitC[i], clus.errorC[i])

	// 	// one message feedback per node
	// 	go func(i int) { clus.proposeC[i] <- "foo" }(i)
	// }

	// for range clus.peers {
	// 	<-donec
	// }
}
