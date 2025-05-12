package xraft

type cluster_grpc struct {
	*cluster
	servers []string
}

// // newCluster creates a cluster of n nodes
// func newCluster_grpc(n int) *cluster_grpc {
// 	peers := make([]string, n)
// 	grpcServers := make([]string, n)
// 	for i := range peers {
// 		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", 11000+i)
// 		grpcServers[i] = fmt.Sprintf("127.0.0.1:%d", 12000+i)
// 	}

// 	gclus := &cluster_grpc{
// 		cluster: &cluster{
// 			peers:              peers,
// 			commitC:            make([]<-chan *commit, len(peers)),
// 			errorC:             make([]<-chan error, len(peers)),
// 			proposeC:           make([]chan string, len(peers)),
// 			stateC:             make([]chan raft.StateType, len(peers)),
// 			confChangeC:        make([]chan raftpb.ConfChange, len(peers)),
// 			snapshotTriggeredC: make([]<-chan struct{}, len(peers)),
// 			state:              make([]*StateMachine, len(peers)),
// 		},
// 		servers: grpcServers,
// 	}

// 	for i := range gclus.peers {
// 		os.RemoveAll(fmt.Sprintf("raftexample-%d", i+1))
// 		os.RemoveAll(fmt.Sprintf("raftexample-%d-snap", i+1))
// 		proposeC := make(chan string, 1)
// 		stateC := make(chan raft.StateType, 1)

// 		gclus.proposeC[i] = proposeC
// 		gclus.stateC[i] = stateC
// 		gclus.confChangeC[i] = make(chan raftpb.ConfChange, 1)
// 		fn, snapshotTriggeredC := getSnapshotFn()
// 		gclus.snapshotTriggeredC[i] = snapshotTriggeredC
// 		commitC, errorC, snapReady := newRaftNode(i+1, gclus.peers, false, fn, gclus.proposeC[i], gclus.stateC[i], gclus.confChangeC[i])
// 		gclus.commitC[i] = commitC
// 		gclus.errorC[i] = errorC
// 		gclus.state[i] = NewStateMachine_grpc(grpcServers[i], i+1, <-snapReady, proposeC, stateC, gclus.commitC[i], gclus.errorC[i])
// 	}

// 	return gclus
// }

// func Test2PCGprc(t *testing.T) {
// 	serverAddr := ":7777"
// 	kv, _ := xkv.NewKVStore("xraft-0.db", "test")
// 	defer kv.Destroy()

// 	p, _, _ := conn.NewParticipants_2PC(0, kv)
// 	defer p.Close()
// 	go conn.Startgrpc(p, serverAddr)

// 	client := NewGrpcClient([]string{serverAddr}, "client-1", 1)

// 	client.Set("key", "val")
// 	val := client.Get("key")
// 	if val != "val" {
// 		t.Errorf("Test2PCGprc() Get = %v, want val", val)
// 	}
// 	time.Sleep(time.Microsecond * 500)
// }

// func TestGrpcCluster(t *testing.T) {
// 	defer cleanPersist()
// 	replica_num := 3
// 	cluster := newCluster_grpc(replica_num)
// 	defer cluster.Close()

// 	client := NewGrpcClient(cluster.servers, "client-1", 1)
// 	defer client.Close()

// 	client.Set("key", "val")
// 	val := client.Get("key")
// 	if val != "val" {
// 		t.Errorf("Test2PCGprc() Get = %v, want val", val)
// 	}
// 	time.Sleep(time.Microsecond * 500)
// }
