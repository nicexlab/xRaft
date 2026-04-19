package main

import (
	"fmt"
	"github/Fischer0522/xraft/xraft"
	"testing"
)

func TestPing(t *testing.T) {
	cluster := newClusterInfo(defaultClientGRPCAddrs)
	client := xraft.NewGrpcClient(cluster.grpcServerAddress, id)
	go benchServer(fmt.Sprintf(":%v", 9360+id), client)
	defer func() {
		fmt.Printf("client %v:", id)
		client.Static()
	}()

	client.PingTest(20)
}

func TestPingAndWrite(t *testing.T) {
	cluster := newClusterInfo(defaultClientGRPCAddrs)
	client := xraft.NewGrpcClient(cluster.grpcServerAddress, id)
	go benchServer(fmt.Sprintf(":%v", 9360+id), client)
	defer func() {
		fmt.Printf("client %v:", id)
		client.Static()
	}()

	client.PingAndWriteTest(20, 1024)
}
