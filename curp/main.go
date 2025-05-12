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
	"flag"
	pb "github/Fischer0522/xraft/curp/curp_proto"
	"log"
	"net"
	"strings"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedCurpServer
}

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	//kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	rpc_port := flag.String("rpc", "http://127.0.0.1:9366", "rpc port")
	flag.Parse()

	proposeC := make(chan string)
	stateC := make(chan raft.StateType)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	lis, err := net.Listen("tcp", *rpc_port)

	if err != nil {
		log.Fatalf("failed to listen rpc port: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterCurpServer(s, &server{})

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

	// raft provides a commit stream for the proposals from the http api
	var kvs *StateMachine
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady := newRaftNode(*id, strings.Split(*cluster, ","), *join, getSnapshot, proposeC, stateC, confChangeC)

	kvs = newState(*id, <-snapshotterReady, proposeC, stateC, commitC, errorC)

	// the key-value http handler will propose updates to raft
	//serveHttpKVAPI(kvs, *kvport, confChangeC, errorC)
}
