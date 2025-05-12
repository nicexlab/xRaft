// // Copyright 2016 The etcd Authors
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.

package xraft

import (
	"fmt"
	"github/Fischer0522/xraft/xraft/conn"
	"os"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// import (
// 	"fmt"
// 	xkv "github/Fischer0522/xraft/kv"
// 	"github/Fischer0522/xraft/xraft/conn"
// 	xproto "github/Fischer0522/xraft/xraft/proto"
// 	"github/Fischer0522/xraft/xraft/test"
// 	"log"
// 	"math/rand"
// 	"os"
// 	"reflect"
// 	"sync"
// 	"testing"
// 	"time"

// 	"go.etcd.io/etcd/raft/v3"
// 	"go.etcd.io/etcd/raft/v3/raftpb"

// 	"google.golang.org/protobuf/proto"
// )

// func Test_kvstore_snapshot(t *testing.T) {
// 	// tm := map[string]string{"foo": "bar"}
// 	tm, _ := xkv.NewKVStore("test.db", "test")
// 	tm.Put("foo", "bar")
// 	s := &StateMachine{kvStore: tm}

// 	v, _ := s.Lookup("foo")
// 	if v != "bar" {
// 		t.Fatalf("foo has unexpected value, got %s", v)
// 	}

// 	data, err := s.getSnapshot()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	s.kvStore = nil

// 	if err := s.recoverFromSnapshot(data); err != nil {
// 		t.Fatal(err)
// 	}
// 	v, _ = s.Lookup("foo")
// 	if v != "bar" {
// 		t.Fatalf("foo has unexpected value, got %s", v)
// 	}
// 	if !reflect.DeepEqual(s.kvStore, tm) {
// 		t.Fatalf("store expected %+v, got %+v", tm, s.kvStore)
// 	}
// }

func cleanPersist() {
	for i := 0; i < 3; i++ {
		err := os.RemoveAll(fmt.Sprintf("./raftexample-%d", i+1))
		if err != nil {
			fmt.Println(err)
		}
		err = os.RemoveAll(fmt.Sprintf("./raftexample-%d-snap", i+1))
		if err != nil {
			fmt.Println(err)
		}
	}
}

func waitElection() {
	time.Sleep(5 * time.Second)
}
func InitSingleCluster() (*StateMachine, chan string, chan raftpb.ConfChange) {
	clusters := []string{"http://127.0.0.1:9021"}

	proposeC := make(chan string, 1)
	stateC := make(chan raft.StateType, 1)

	confChangeC := make(chan raftpb.ConfChange, 1)

	var kvs *StateMachine
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady := newRaftNode(1, clusters, false, getSnapshot, proposeC, stateC, confChangeC)

	kvs = NewStateMachine(1, <-snapshotterReady, proposeC, stateC, commitC, errorC)
	return kvs, proposeC, confChangeC
}

func InitSingleClient(s []*StateMachine, id int) *Client {
	peers := make([]*conn.Participants_2PC, len(s))
	for i := range peers {
		peers[i] = s[i].proxy
	}
	// co := conn.Init_Coordinator_2PC(client_trans, name)
	client := newClient(peers, id)
	return client
}

// func generateCommand(key string, rand *rand.Rand) *xproto.Message {
// 	mess := &xproto.Message{Nonce: rand.Uint64()}
// 	mess.PrepareTxs = &xproto.Transactions{}
// 	mess.PrepareTxs.Trans = make([]*xproto.Transaction, 1)
// 	mess.PrepareTxs.Trans[0] = test.GenerateRandomTransaction(rand)
// 	mess.PrepareTxs.Trans[0].Command.Key = key
// 	mess.PrepareTxs.Trans[0].Command.Op = xproto.PUT
// 	return mess
// }

// func sendMessageWithKey(key string, tran conn.Trans, rand *rand.Rand) *xproto.Message {
// 	m1 := generateCommand(key, rand)
// 	buf, _ := proto.Marshal(m1)
// 	conn.BufWrite(tran, buf) // client1 send a command
// 	return m1
// }

// func receiveMessageReply(tran conn.Trans) *xproto.MessageReply {
// 	buf_reply, _ := conn.BufRead(tran)
// 	m1_reply := &xproto.MessageReply{}
// 	proto.Unmarshal(buf_reply, m1_reply)
// 	return m1_reply
// }

// func Test_xraft_setKeyToSlow(t *testing.T) {
// 	defer cleanPersist()

// 	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
// 	// key, value := "key", "value"
// 	kvs, proposeC, confC := InitSingleCluster()
// 	defer close(proposeC)
// 	defer close(confC)
// 	waitElection()
// 	client1_tran, server1_tran := test.NewIoHelper(make(chan byte, 1), make(chan byte, 1))
// 	client2_tran, server2_tran := test.NewIoHelper(make(chan byte, 1), make(chan byte, 1))
// 	kvs.ClientRegister("client-1", server1_tran)
// 	kvs.ClientRegister("client-2", server2_tran)

// 	sendMessageWithKey("key3", client1_tran, rand)

// 	m1_reply := receiveMessageReply(client1_tran)
// 	if (m1_reply.PrepareTxs[0].R & (1<<8 - 1)) != uint32(xproto.PREPARE_SUCCEED) {
// 		t.Errorf("Test_xraft_slowcmd() = first command not accepted !")
// 	}

// 	// client2 send a command m2 conflict with m1
// 	m2 := sendMessageWithKey("key3", client2_tran, rand)

// 	m2_reply := receiveMessageReply(client2_tran)

// 	if (m2_reply.PrepareTxs[0].R & (1<<8 - 1)) != uint32(xproto.PREPARE_CONFLICT) {
// 		t.Errorf("Test_xraft_slowcmd() = second command detects not conflict !")
// 	}
// 	// client abort之前发送的命令
// 	abort_mess := &xproto.Message{}
// 	abort_mess.AbortTxs = &xproto.TransactionNonces{Nonce: make([]uint64, 1)}
// 	abort_mess.AbortTxs.Nonce[0] = m2.PrepareTxs.Trans[0].Nonce
// 	buf_abo, _ := proto.Marshal(abort_mess)
// 	conn.BufWrite(client2_tran, buf_abo) // client2 send a command

// 	abo_reply := receiveMessageReply(client2_tran)
// 	if len(abo_reply.AbortTxs) != 1 {
// 		t.Errorf("Test_xraft_slowcmd() = abort message reply length error %v !", abo_reply)
// 	}
// 	if abo_reply.AbortTxs[0].Nonce != abort_mess.AbortTxs.Nonce[0] {
// 		t.Errorf("Test_xraft_slowcmd() = abort message reply nonce error %v !", abo_reply)
// 	}

// 	conflict_reply := receiveMessageReply(client1_tran)
// 	if len(conflict_reply.SlowTxs) != 1 {
// 		t.Errorf("Test_xraft_slowcmd() = slow path reply length error %v !", conflict_reply)
// 	}
// 	if (conflict_reply.SlowTxs[0].R & (1<<8 - 1)) != uint32(xproto.SLOW_SUCCEED) {
// 		t.Errorf("Test_xraft_slowcmd() = second command slow path fail!")
// 	}
// 	log.Printf("get slow cmd 1 reply %v.", conflict_reply)

// 	conflict_reply2 := receiveMessageReply(client2_tran)
// 	// buf_slowreply2, _ := conn.BufRead(client2_tran)
// 	// conflict_reply2 := &xproto.MessageReply{}
// 	// proto.Unmarshal(buf_slowreply2, conflict_reply2)
// 	log.Printf("get slow cmd 2 reply %v.", conflict_reply2)
// 	if len(conflict_reply2.SlowTxs) != 1 {
// 		t.Errorf("Test_xraft_slowcmd() = slow path reply length error %v !", conflict_reply2)
// 	}
// 	if (conflict_reply2.SlowTxs[0].R & (1<<8 - 1)) != uint32(xproto.SLOW_SUCCEED) {
// 		t.Errorf("Test_xraft_slowcmd() = second command slow path fail!")
// 	}

// 	sendMessageWithKey("key1", client2_tran, rand)
// 	receiveMessageReply(client2_tran)
// 	waitElection()
// 	sendMessageWithKey("key3", client2_tran, rand)
// 	m3_reply := receiveMessageReply(client2_tran)
// 	if (m3_reply.PrepareTxs[0].R & (1<<8 - 1)) != uint32(xproto.PREPARE_SUCCEED) {
// 		t.Errorf("Test_xraft_slowcmd() = first command not accepted !")
// 	}

// 	waitElection()
// }

// func TestSimple(t *testing.T) {
// 	defer cleanPersist()
// 	client_num := 2

// 	tx_num := 20
// 	client_parallel := 2
// 	// rand := rand.New(rand.NewSource(time.Now().UnixNano()))
// 	rand := rand.New(rand.NewSource(1122))
// 	// key, value := "key", "value"
// 	serverlist := make([]*StateMachine, 0)
// 	kvs, proposeC, confC := InitSingleCluster()
// 	serverlist = append(serverlist, kvs)
// 	clientlist := make([]*Client, client_num)
// 	io_fn := make([][]*test.IoHelper, client_num)
// 	defer close(proposeC)
// 	defer close(confC)
// 	waitElection()

// 	for i := 0; i < client_num; i++ {
// 		clientlist[i], io_fn[i] = InitSingleClient(serverlist, fmt.Sprint("client-", i))
// 	}

// 	// txs := make([]*xproto.Transaction, tx_num)
// 	// for i := range txs {
// 	// 	txs[i] = test.GenerateRandomTransaction(rand)
// 	// 	txs[i].Nonce = txs[i].Nonce % 1000
// 	// }
// 	keys := make([][][]string, client_num)
// 	for i := range keys {
// 		keys[i] = make([][]string, client_parallel)
// 		for j := range keys[i] {
// 			keys[i][j] = make([]string, tx_num/client_num/client_parallel)
// 			for k := range keys[i][j] {
// 				keys[i][j][k] = test.RandomString(5, rand)
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
// 					wg.Done()
// 				}(i)
// 			}
// 		}(i)
// 	}

// 	wg.Wait()
// 	time.Sleep(time.Second)
// 	for i := 0; i < client_num; i++ {
// 		for _, io := range io_fn[i] {
// 			io.Close()
// 		}
// 		clientlist[i].Close()
// 	}

// }

// func TestSlowPath(t *testing.T) {
// 	defer cleanPersist()
// 	client_num := 2
// 	client_parallel := 1
// 	tx_num := 20

// 	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
// 	// key, value := "key", "value"
// 	serverlist := make([]*StateMachine, 0)
// 	kvs, proposeC, confC := InitSingleCluster()
// 	serverlist = append(serverlist, kvs)
// 	clientlist := make([]*Client, client_num)
// 	io_fn := make([][]*test.IoHelper, client_num)
// 	defer close(proposeC)
// 	defer close(confC)
// 	waitElection()

// 	for i := 0; i < client_num; i++ {
// 		clientlist[i], io_fn[i] = InitSingleClient(serverlist, fmt.Sprint("client-", i))
// 	}

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
// 					wg.Done()
// 				}(i)
// 			}
// 		}(i)
// 	}

// 	wg.Wait()
// 	time.Sleep(time.Second)
// 	for i := 0; i < client_num; i++ {
// 		for _, io := range io_fn[i] {
// 			io.Close()
// 		}
// 		clientlist[i].Close()
// 	}

// }

// func TestFast(t *testing.T) {
// 	defer cleanPersist()
// 	client_num := 2
// 	client_parallel := 1
// 	tx_num := 10

// 	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
// 	serverlist := make([]*StateMachine, 0)
// 	kvs, proposeC, confC := InitSingleCluster()
// 	serverlist = append(serverlist, kvs)
// 	clientlist := make([]*Client, client_num)
// 	io_fn := make([][]*test.IoHelper, client_num)
// 	defer close(proposeC)
// 	defer close(confC)
// 	waitElection()

// 	for i := 0; i < client_num; i++ {
// 		clientlist[i], io_fn[i] = InitSingleClient(serverlist, fmt.Sprint("client-", i))
// 	}

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
// 					wg.Done()
// 				}(i)
// 			}
// 		}(i)
// 	}

// 	wg.Wait()
// 	time.Sleep(time.Second * 1)
// 	for i := 0; i < client_num; i++ {
// 		for _, io := range io_fn[i] {
// 			io.Close()
// 		}
// 		clientlist[i].Close()
// 	}
// }
