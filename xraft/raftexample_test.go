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

package xraft

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

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
		clus.state[i] = NewStateMachine(i+1, <-snapReady, proposeC, stateC, clus.commitC[i], clus.errorC[i])
	}

	return clus
}

// func (clus *cluster) CheckLeader() (int, bool) {
// 	leader_id := -1

// 	for i := 0; i < len(clus.peers); i++ {
// 		if clus.state[i].raftState == raft.StateLeader {
// 			leader_id = i
// 			break
// 		}
// 	}
// 	if leader_id == -1 {
// 		return leader_id, false
// 	} else {
// 		return leader_id, true
// 	}
// }

// Close closes all cluster nodes and returns an error if any failed.
func (clus *cluster) Close() (err error) {
	time.Sleep(time.Second)
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

// func (clus *cluster) closeNoErrors(t *testing.T) {
// 	t.Log("closing cluster...")
// 	if err := clus.Close(); err != nil {
// 		t.Fatal(err)
// 	}
// 	t.Log("closing cluster [done]")
// }

func TestClusterWithRandomKey(t *testing.T) {
	defer cleanPersist()
	client_num := 2
	tx_num := 200
	replica_num := 3

	cluster := newCluster(replica_num)
	defer cluster.Close()
	// key, value := "key", "value"

	clientlist := make([]*Client, client_num)

	waitElection()

	for i := 0; i < client_num; i++ {
		clientlist[i] = InitSingleClient(cluster.state, i)
	}

	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	keys := make([][]string, client_num)
	for i := range keys {
		keys[i] = make([]string, tx_num/client_num)
		for j := range keys[i] {
			keys[i][j] = RandomString(5, rand)
		}
	}

	vals := make([][]string, client_num)
	for i := range keys {
		vals[i] = make([]string, tx_num/client_num)
		for j := range keys[i] {
			vals[i][j] = RandomString(5, rand)
		}
	}

	var wg sync.WaitGroup
	wg.Add(client_num)
	for i := 0; i < client_num; i++ {
		go func(cid int) {
			for j := range keys[cid] {
				clientlist[cid].Set(keys[cid][j], vals[cid][j])
				t.Log("succeed")
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	time.Sleep(time.Second * 5)
}

func TestClusterWithSameKey(t *testing.T) {
	defer cleanPersist()
	client_num := 20
	tx_num := 200
	replica_num := 3

	cluster := newCluster(replica_num)
	defer cluster.Close()
	// key, value := "key", "value"

	clientlist := make([]*Client, client_num)

	waitElection()

	for i := 0; i < client_num; i++ {
		clientlist[i] = InitSingleClient(cluster.state, i)
	}

	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	keys := make([][]string, client_num)
	for i := range keys {
		keys[i] = make([]string, tx_num/client_num)
		for j := range keys[i] {
			keys[i][j] = "key"
		}
	}

	vals := make([][]string, client_num)
	for i := range keys {
		vals[i] = make([]string, tx_num/client_num)
		for j := range keys[i] {
			vals[i][j] = RandomString(5, rand)
		}
	}

	var wg sync.WaitGroup
	wg.Add(client_num)
	for i := 0; i < client_num; i++ {
		go func(cid int) {
			for j := range keys[cid] {
				clientlist[cid].Set(keys[cid][j], vals[cid][j])
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	time.Sleep(time.Second * 5)
}

// func TestClusterSlowKey(t *testing.T) {
// 	defer cleanPersist()
// 	client_num := 5
// 	client_parallel := 1
// 	tx_num := 100
// 	replica_num := 3

// 	cluster := newCluster(replica_num)
// 	defer cluster.Close()
// 	// key, value := "key", "value"

// 	clientlist := make([]*Client, client_num)
// 	io_fn := make([][]*test.IoHelper, client_num)
// 	waitElection()

// 	for i := 0; i < client_num; i++ {
// 		clientlist[i], io_fn[i] = InitSingleClient(cluster.state, fmt.Sprint("client-", i))
// 	}

// 	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
// 	keys := make([][][]string, client_num)
// 	for i := range keys {
// 		keys[i] = make([][]string, client_parallel)
// 		for j := range keys[i] {
// 			keys[i][j] = make([]string, tx_num/client_num/client_parallel)
// 			for k := range keys[i][j] {
// 				keys[i][j][k] = "conflictKey"
// 			}
// 		}
// 	}

// 	vals := make([][][]string, client_num)
// 	for i := range vals {
// 		vals[i] = make([][]string, client_parallel)
// 		for j := range vals[i] {
// 			vals[i][j] = make([]string, tx_num/client_num/client_parallel)
// 			for k := range vals[i][j] {
// 				vals[i][j][k] = test.RandomString(5, rand)
// 			}
// 		}
// 	}

// 	var wg sync.WaitGroup
// 	wg.Add(client_parallel * client_num)
// 	for i := 0; i < client_num; i++ {
// 		go func(client_id int) {
// 			for i := 0; i < client_parallel; i++ {
// 				go func(id int) {
// 					for j := range keys[client_id][id] {
// 						clientlist[client_id].Set(keys[client_id][id][j], vals[client_id][id][j])
// 					}
// 					log.Printf("client %v %v-th thread end", client_id, id)
// 					wg.Done()
// 				}(i)
// 			}
// 		}(i)
// 	}

// 	wg.Wait()
// 	time.Sleep(time.Second * 5)
// 	for i := 0; i < client_num; i++ {
// 		for _, io := range io_fn[i] {
// 			io.Close()
// 		}
// 		clientlist[i].Close()
// 	}
// }

// func TestClusterFastKey(t *testing.T) {
// 	defer cleanPersist()
// 	client_num := 5
// 	client_parallel := 1
// 	tx_num := 1000
// 	replica_num := 3

// 	cluster := newCluster(replica_num)
// 	defer cluster.Close()
// 	// key, value := "key", "value"

// 	clientlist := make([]*Client, client_num)
// 	io_fn := make([][]*test.IoHelper, client_num)
// 	waitElection()

// 	for i := 0; i < client_num; i++ {
// 		clientlist[i], io_fn[i] = InitSingleClient(cluster.state, fmt.Sprint("client-", i))
// 	}

// 	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
// 	keys := make([][][]string, client_num)
// 	for i := range keys {
// 		keys[i] = make([][]string, client_parallel)
// 		for j := range keys[i] {
// 			keys[i][j] = make([]string, tx_num/client_num/client_parallel)
// 			for k := range keys[i][j] {
// 				if k == 0 || k == len(keys[i][j])-1 {
// 					keys[i][j][k] = "conflictKey"
// 				} else {
// 					keys[i][j][k] = test.RandomString(3, rand)
// 				}
// 			}
// 		}
// 	}

// 	vals := make([][][]string, client_num)
// 	for i := range vals {
// 		vals[i] = make([][]string, client_parallel)
// 		for j := range vals[i] {
// 			vals[i][j] = make([]string, tx_num/client_num/client_parallel)
// 			for k := range vals[i][j] {
// 				vals[i][j][k] = test.RandomString(5, rand)
// 			}
// 		}
// 	}

// 	var wg sync.WaitGroup
// 	wg.Add(client_parallel * client_num)
// 	for i := 0; i < client_num; i++ {
// 		go func(client_id int) {
// 			for i := 0; i < client_parallel; i++ {
// 				go func(id int) {
// 					for j := range keys[client_id][id] {
// 						clientlist[client_id].Set(keys[client_id][id][j], vals[client_id][id][j])
// 					}
// 					log.Printf("client %v %v-th thread end", client_id, id)
// 					wg.Done()
// 				}(i)
// 			}
// 		}(i)
// 	}

// 	wg.Wait()
// 	time.Sleep(time.Second * 5)
// 	for i := 0; i < client_num; i++ {
// 		for _, io := range io_fn[i] {
// 			io.Close()
// 		}
// 		clientlist[i].Close()
// 	}
// }
