package main

import (
	"fmt"
	"github/Fischer0522/xraft/xraft"
	"testing"
)

func TestPing(t *testing.T) {
	clus_info := &clus_info{
		grpcserver_address: []string{"192.168.0.203:11041", "192.168.0.204:11041", "192.168.0.206:11041"},
		// grpcserver_address: []string{":8020", ":8021", ":8022"},
	}
	client := xraft.NewGrpcClient(clus_info.grpcserver_address, id)
	go bench_server(fmt.Sprintf(":%v", 9360+id), client)
	defer func() {
		fmt.Printf("client %v:", id)
		client.Static()
	}()

	client.PingTest(20)
}

func TestPingAndWrite(t *testing.T) {
	clus_info := &clus_info{
		grpcserver_address: []string{"192.168.0.203:11041", "192.168.0.204:11041", "192.168.0.206:11041"},
		// grpcserver_address: []string{":8020", ":8021", ":8022"},
	}
	client := xraft.NewGrpcClient(clus_info.grpcserver_address, id)
	go bench_server(fmt.Sprintf(":%v", 9360+id), client)
	defer func() {
		fmt.Printf("client %v:", id)
		client.Static()
	}()

	client.PingAndWriteTest(20, 1024)
}
