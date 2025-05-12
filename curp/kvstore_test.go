// Copyright 2016 The etcd Authors
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
	"sync"
	"testing"

	"github/Fischer0522/xraft/curp/command"
	"github/Fischer0522/xraft/curp/curp_proto"

	"go.etcd.io/etcd/raft/v3"
)

func InitState() *StateMachine {
	proposeC := make(chan string, 1)
	stateC := make(chan raft.StateType, 1)
	commitC := make(chan *commit, 1)
	errorC := make(chan error, 1)
	state := newState(1, nil, proposeC, stateC, commitC, errorC)
	return state
}

// just a simple test,test whether the fast path is blocked
func TestStateSimplePut(t *testing.T) {

	state := InitState()
	proposeId := command.ProposeId{
		ClientId: 1,
		SeqId:    1,
	}

	key, val := "k1", "v1"
	cmd := &curp_proto.CurpClientCommand{
		Op:       command.PUT,
		Key:      key,
		Value:    val,
		ClientId: proposeId.ClientId,
		SeqId:    proposeId.SeqId,
	}
	reply := state.Propose(cmd)

	if reply.StatusCode != curp_proto.ACCEPTED && reply.Content != "" {
		t.Fatal("Propose failed")
	}
}

func TestStateGet(t *testing.T) {
	state := InitState()
	key, val := "k1", "v1"

	// if we don't launch the raft Node,the gc will not work,
	//  when proposing a key ,the result will be conflict forever
	// so,in this test, we need to insert a key into the kvStore directly
	state.KVStore.Put(key, val)
	proposeId := command.ProposeId{
		ClientId: 1,
		SeqId:    2,
	}

	getCmd := &curp_proto.CurpClientCommand{
		Op:       command.GET,
		Key:      key,
		ClientId: proposeId.ClientId,
		SeqId:    proposeId.SeqId,
	}

	reply := state.Lookup(getCmd)
	if reply.StatusCode != curp_proto.ACCEPTED {
		t.Fatal("Propose failed")
	}
	if reply.Content != val {
		t.Fatalf("wrong value,expected: %s, got: %s", val, reply.Content)
	}

}

func TestStatePutMulti(t *testing.T) {
	state := InitState()

	for i := 0; i < 10000; i++ {
		proposeId := command.ProposeId{
			ClientId: uint64(i),
			SeqId:    uint64(i),
		}
		key, val := fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)
		cmd := &curp_proto.CurpClientCommand{
			Op:       command.PUT,
			Key:      key,
			Value:    val,
			ClientId: proposeId.ClientId,
			SeqId:    proposeId.SeqId,
		}
		reply := state.Propose(cmd)

		if reply.StatusCode != curp_proto.ACCEPTED && reply.Content != "" {
			t.Fatal("Propose failed")
		}
	}
}

func TestStateGetMulti(t *testing.T) {
	state := InitState()

	for i := 0; i < 10000; i++ {
		key, val := fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)
		// state.kvStore[key] = val
		state.KVStore.Put(key, val)
	}

	for i := 0; i < 10000; i++ {
		proposeId := command.ProposeId{
			ClientId: uint64(i),
			SeqId:    uint64(i),
		}
		key, val := fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)
		getCmd := &curp_proto.CurpClientCommand{
			Op:       command.GET,
			Key:      key,
			Value:    val,
			ClientId: proposeId.ClientId,
			SeqId:    proposeId.SeqId,
		}
		reply := state.Lookup(getCmd)
		if reply.StatusCode != curp_proto.ACCEPTED {
			t.Fatal("Propose failed")
		}
		if reply.Content != val {
			t.Fatalf("wrong value,expected: %s, got: %s", val, reply.Content)
		}
	}
}

func TestStateConcurrentPut(t *testing.T) {
	state := InitState()
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			for j := 0; j < 100; j++ {
				proposeId := command.ProposeId{
					ClientId: uint64(i),
					SeqId:    uint64(j),
				}
				key, val := fmt.Sprintf("k%d", i*100+j), fmt.Sprintf("v%d", i*100+j)
				cmd := &curp_proto.CurpClientCommand{
					Op:       command.PUT,
					Key:      key,
					Value:    val,
					ClientId: proposeId.ClientId,
					SeqId:    proposeId.SeqId,
				}

				reply := state.Propose(cmd)

				if reply.StatusCode != curp_proto.ACCEPTED && reply.Content != "" {
					t.Error("Propose failed")
					t.Fail()
				}

			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func TestStateConcurrentGet(t *testing.T) {
	state := InitState()
	var wg sync.WaitGroup
	for i := 0; i < 20000; i++ {
		key, val := fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)
		// state.kvStore[key] = val
		state.KVStore.Put(key, val)
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			for j := 0; j < 100; j++ {
				proposeId := command.ProposeId{
					ClientId: uint64(i),
					SeqId:    uint64(j),
				}
				key, val := fmt.Sprintf("k%d", i*100+j), fmt.Sprintf("v%d", i*100+j)
				getCmd := &curp_proto.CurpClientCommand{
					Op:       command.GET,
					Key:      key,
					Value:    val,
					ClientId: proposeId.ClientId,
					SeqId:    proposeId.SeqId,
				}
				reply := state.Lookup(getCmd)
				if reply.StatusCode != curp_proto.ACCEPTED {
					t.Fatal("Propose failed")
				}
				if reply.Content != val {
					t.Fatalf("wrong value,expected: %s, got: %s", val, reply.Content)
				}

			}
			wg.Done()
		}(i)
	}
	wg.Wait()
}
