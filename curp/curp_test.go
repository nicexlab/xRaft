package curp

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github/Fischer0522/xraft/curp/command"
	"github/Fischer0522/xraft/curp/curp_proto"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestBase(t *testing.T) {
	clus := newCluster(1)
	// close before replay so raft never starts
	defer clus.closeNoErrors(t)
}

func waitElection() {
	time.Sleep(2 * time.Second)
}

func clientCmd(clientId uint64, seqId uint64, op uint32, key string, value string) *curp_proto.CurpClientCommand {
	proposeId := command.ProposeId{
		ClientId: clientId,
		SeqId:    seqId,
	}
	return &curp_proto.CurpClientCommand{
		Op:       op,
		Key:      key,
		Value:    value,
		ClientId: proposeId.ClientId,
		SeqId:    proposeId.SeqId,
	}

}

func cleanPersist() {
	for i := 0; i < 3; i++ {
		os.RemoveAll(fmt.Sprintf("raftexample-%d", i+1))
		os.RemoveAll(fmt.Sprintf("raftexample-%d-snap", i+1))
	}

}

func InitSingleCluster() (*StateMachine, chan string, chan raftpb.ConfChange) {
	clusters := []string{"http://127.0.0.1:9021"}

	proposeC := make(chan string, 1)
	stateC := make(chan raft.StateType, 1)

	confChangeC := make(chan raftpb.ConfChange, 1)

	var kvs *StateMachine
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady := newRaftNode(1, clusters, false, getSnapshot, proposeC, stateC, confChangeC)

	kvs = newState(1, <-snapshotterReady, proposeC, stateC, commitC, errorC)
	return kvs, proposeC, confChangeC
}

// single node test -------------------------------------------
func TestServerPut(t *testing.T) {

	defer cleanPersist()
	key, value := "key", "value"
	cmd := clientCmd(0, 0, command.PUT, key, value)
	state, proposeC, confC := InitSingleCluster()
	defer close(proposeC)
	defer close(confC)
	waitElection()
	result := state.Propose(cmd)
	if result.StatusCode != curp_proto.ACCEPTED {
		t.Fatal("Propose failed")
	}
	if result.Content != "" {
		t.Fatal("Propose result is not empty")
	}
}

func TestServerProposeMulti(t *testing.T) {
	defer cleanPersist()
	number := 100
	state, proposeC, confC := InitSingleCluster()
	defer close(proposeC)
	defer close(confC)
	waitElection()

	for i := 0; i < number; i++ {
		key, value := fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)
		cmd := clientCmd(0, uint64(i), command.PUT, key, value)
		reply := state.Propose(cmd)
		if reply.StatusCode != curp_proto.ACCEPTED {
			t.Fatal("Propose failed")
		}
		if reply.Content != "" {
			t.Fatal("Propose result is not empty")
		}
	}
}

func TestServerHandleConflict(t *testing.T) {
	defer cleanPersist()
	state, proposeC, confC := InitSingleCluster()
	// wait for raft init
	waitElection()
	defer close(proposeC)
	defer close(confC)

	key, value := "key", "value"
	putCmd := clientCmd(0, 0, command.PUT, key, value)
	getCmd := clientCmd(0, 1, command.GET, key, value)

	reply := state.Propose(putCmd)
	if reply.StatusCode != curp_proto.ACCEPTED {
		t.Fatal("Propose failed")
	}
	if reply.Content != "" {
		t.Fatal("Propose result is not empty")
	}

	time.Sleep(50 * time.Millisecond)
	reply = state.Lookup(getCmd)
	if reply.StatusCode == curp_proto.CONFLICT {
		t.Fatal("Conflict should be already handled")
	}
}

func TestServerProposeGetMulti(t *testing.T) {
	defer cleanPersist()
	number := 1000
	state, proposeC, confC := InitSingleCluster()
	defer close(proposeC)
	defer close(confC)
	waitElection()

	for i := 0; i < number; i++ {
		key, value := fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)
		cmd := clientCmd(0, uint64(i), command.PUT, key, value)
		reply := state.Propose(cmd)
		if reply.StatusCode != curp_proto.ACCEPTED {
			t.Fatal("Propose failed")
		}
		if reply.Content != "" {
			t.Fatal("Propose result is not empty")
		}
	}
	// wait for committed
	time.Sleep(50 * time.Millisecond)

	for i := 0; i < number; i++ {
		key, value := fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)
		cmd := clientCmd(0, uint64(i+number), command.GET, key, value)
		reply := state.Lookup(cmd)
		if reply.StatusCode != curp_proto.ACCEPTED {
			t.Fatal("Propose failed")
		}
		if reply.Content != value {
			t.Fatalf("wrong value,expected: %s, got: %s", value, reply.Content)
		}
	}
}

func TestServerConcurrentRead(t *testing.T) {
	defer cleanPersist()
	threadNum := 10
	number := 100
	state, _, _ := InitSingleCluster()
	//defer close(proposeC)
	//defer close(confC)
	waitElection()
	for i := 0; i < threadNum; i++ {
		for j := 0; j < number; j++ {
			key, value := fmt.Sprintf("key-%d-%d", i, j), fmt.Sprintf("value-%d-%d", i, j)
			cmd := clientCmd(uint64(i), uint64(j), command.PUT, key, value)
			reply := state.Propose(cmd)
			if reply.StatusCode != curp_proto.ACCEPTED {
				t.Fatal("Propose failed")
			}
			if reply.Content != "" {
				t.Fatal("Propose result is not empty")
			}
		}
	}
	// wait slow path to gc witness
	time.Sleep(1 * time.Second)

	var wg_read sync.WaitGroup
	for i := 0; i < threadNum; i++ {
		wg_read.Add(1)
		go func(clientId uint64) {
			for j := 0; j < number; j++ {
				key, value := fmt.Sprintf("key-%d-%d", clientId, j), fmt.Sprintf("value-%d-%d", clientId, j)
				cmd := clientCmd(clientId, uint64(j+number), command.GET, key, value)
				reply := state.Lookup(cmd)
				if reply.StatusCode != curp_proto.ACCEPTED {
					t.Fatal("Propose failed")
				}
				if reply.Content != value {
					t.Fatalf("wrong value,expected: %s, got: %s", value, reply.Content)
				}

			}
			wg_read.Done()
		}(uint64(i))
	}
	wg_read.Wait()
}

func TestServerConcurrentWrite(t *testing.T) {
	defer cleanPersist()
	threadNum := 10
	number := 10
	state, proposeC, confC := InitSingleCluster()
	defer close(proposeC)
	defer close(confC)
	waitElection()

	var wg sync.WaitGroup
	for i := 0; i < threadNum; i++ {
		wg.Add(1)
		go func(i int) {
			for j := 0; j < number; j++ {
				key, value := fmt.Sprintf("key-%d-%d", i, j), fmt.Sprintf("value-%d-%d", i, j)
				cmd := clientCmd(uint64(i), uint64(j), command.PUT, key, value)
				reply := state.Propose(cmd)
				if reply.StatusCode != curp_proto.ACCEPTED {
					t.Fatal("Propose failed")
				}
				if reply.Content != "" {
					t.Fatal("Propose result is not empty")
				}
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	// wait slow path to gc witness
	time.Sleep(1 * time.Second)

	for i := 0; i < threadNum; i++ {
		for j := 0; j < number; j++ {
			key, value := fmt.Sprintf("key-%d-%d", i, j), fmt.Sprintf("value-%d-%d", i, j)
			cmd := clientCmd(uint64(i), uint64(j+number), command.GET, key, "")
			reply := state.Lookup(cmd)
			if reply.StatusCode != curp_proto.ACCEPTED {
				t.Fatal("Propose failed")
			}
			if reply.Content != value {
				t.Fatalf("wrong value,expected: %s, got: %s", value, reply.Content)
			}
		}
	}
}

func TestServerConcurrent(t *testing.T) {
	defer cleanPersist()
	threadNum := 10
	number := 1000
	state, proposeC, confC := InitSingleCluster()
	defer close(proposeC)
	defer close(confC)
	waitElection()
	var wg_write sync.WaitGroup
	for i := 0; i < threadNum; i++ {
		wg_write.Add(1)
		go func(clientId uint64) {
			for j := 0; j < number; j++ {
				key, value := fmt.Sprintf("key-%d-%d", clientId, j), fmt.Sprintf("value-%d-%d", clientId, j)
				cmd := clientCmd(clientId, uint64(j), command.PUT, key, value)
				reply := state.Propose(cmd)
				if reply.StatusCode != curp_proto.ACCEPTED {
					t.Fatal("Propose failed")
				}
				if reply.Content != "" {
					t.Fatal("Propose result is not empty")
				}
			}
			wg_write.Done()
		}(uint64(i))
	}
	wg_write.Wait()
	// wait slow path to gc witness
	time.Sleep(1 * time.Second)

	var wg_read sync.WaitGroup
	for i := 0; i < threadNum; i++ {
		wg_read.Add(1)
		go func(clientId uint64) {
			for j := 0; j < number; j++ {
				key, value := fmt.Sprintf("key-%d-%d", clientId, j), fmt.Sprintf("value-%d-%d", clientId, j)
				cmd := clientCmd(clientId, uint64(j+number), command.GET, key, value)
				reply := state.Lookup(cmd)
				if reply.StatusCode != curp_proto.ACCEPTED {
					t.Fatal("Propose failed")
				}
				if reply.Content != value {
					t.Fatalf("wrong value,expected: %s, got: %s", value, reply.Content)
				}
			}
			wg_read.Done()
		}(uint64(i))
	}
	wg_read.Wait()
}

// cluster test,we state 3 raft node to launch a cluster

func TestClusterBase(t *testing.T) {
	defer cleanPersist()
	clus := newCluster(3)
	//defer clus.closeNoErrors(t)
	waitElection()
	reply := clus.state[0].Propose(clientCmd(1, 1, command.PUT, "a", "1"))

	if reply.StatusCode != curp_proto.ACCEPTED {
		t.Fatal("Propose failed")
	}
	if reply.Content != "" {
		t.Fatal("Propose result is not empty")
	}

}

// propose all commands to leader
func TestClusterProposeMulti(t *testing.T) {
	defer cleanPersist()
	clus := newCluster(3)
	//defer clus.closeNoErrors(t)
	number := 100
	waitElection()
	// leaderId is used for wait synced function
	leaderId, ok := clus.CheckLeader()
	if !ok {
		t.Fatal("no leader")
	}
	state := clus.state[leaderId]
	for i := 0; i < number; i++ {
		key, value := fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)
		cmd := clientCmd(0, uint64(i), command.PUT, key, value)
		reply := state.Propose(cmd)

		if reply.StatusCode != curp_proto.ACCEPTED {
			t.Fatal("Propose failed")
		}
		if reply.Content != "" {
			t.Fatal("Propose result is not empty")
		}
	}
	// wait for committed
	time.Sleep(250 * time.Millisecond)

	for i := 0; i < number; i++ {
		key, value := fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)
		cmd := clientCmd(0, uint64(i+number), command.GET, key, value)
		reply := state.Lookup(cmd)
		if reply.StatusCode != curp_proto.ACCEPTED {
			t.Fatal("Propose failed")
		}
		if reply.Content != value {
			t.Fatalf("wrong value,expected: %s, got: %s", value, reply.Content)
		}
	}
}

func TestClusterProposeConcurrent(t *testing.T) {
	defer cleanPersist()
	clus := newCluster(3)
	//defer clus.closeNoErrors(t)
	threadNum := 5
	number := 1000
	waitElection()
	// leaderId is used for wait synced function
	leaderId, ok := clus.CheckLeader()
	if !ok {
		t.Fatal("no leader")
	}
	state := clus.state[leaderId]

	var wg_write sync.WaitGroup
	for i := 0; i < threadNum; i++ {
		wg_write.Add(1)
		go func(clientId uint64) {
			for j := 0; j < number; j++ {
				key, value := fmt.Sprintf("key-%d-%d", clientId, j), fmt.Sprintf("value-%d-%d", clientId, j)
				cmd := clientCmd(clientId, uint64(j), command.PUT, key, value)
				reply := state.Propose(cmd)

				if reply.StatusCode != curp_proto.ACCEPTED {
					t.Fatal("Propose failed")
				}
				if reply.Content != "" {
					t.Fatal("Propose result is not empty")
				}
			}
			wg_write.Done()
		}(uint64(i))
	}
	wg_write.Wait()
	// wait slow path to gc witness
	time.Sleep(1 * time.Second)

	var wg_read sync.WaitGroup
	for i := 0; i < threadNum; i++ {
		wg_read.Add(1)
		go func(clientId uint64) {
			for j := 0; j < number; j++ {
				key, value := fmt.Sprintf("key-%d-%d", clientId, j), fmt.Sprintf("value-%d-%d", clientId, j)
				cmd := clientCmd(clientId, uint64(j+number), command.GET, key, value)
				reply := state.Lookup(cmd)
				if reply.StatusCode != curp_proto.ACCEPTED {
					t.Fatal("Propose failed")
				}
				if reply.Content != value {
					t.Fatalf("wrong value,expected: %s, got: %s", value, reply.Content)
				}
			}
			wg_read.Done()
		}(uint64(i))
	}
	wg_read.Wait()
}
