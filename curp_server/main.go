package main

import (
	"flag"
	"fmt"
	"github/Fischer0522/xraft/curp"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

type clus_info struct {
	server_address     []string
	grpcserver_address []string
}

var (
	runcluster = flag.Bool("s", false, "start a single peer or the cluster?")
	peers      = flag.String("addrs", "http://127.0.0.1:7010,http://127.0.0.1:7011,http://127.0.0.1:7012", "address of raftexample")
	grpc_peers = flag.String("gaddrs", ":8020,:8021,:8022", "address of grpc server")

	id        = flag.Int("id", 0, "id of the replica")
	grpc_addr = flag.String("gport", ":11041", "port of the grpc server")
)

func main() {

	flag.Parse()

	if !*runcluster {
		clus_info := &clus_info{
			server_address:     strings.Split(*peers, ","),
			grpcserver_address: strings.Split(*grpc_peers, ","),
		}
		number := len(clus_info.grpcserver_address)
		if number != len(clus_info.server_address) {
			log.Fatal("flag error")
		}
		states := make([]*curp.StateMachine, number)
		for i := range states {
			stat, close_peer := curp.RunStateMachine(clus_info.server_address, clus_info.grpcserver_address[i], i+1)
			states[i] = stat
			defer close_peer()
		}
	} else {
		clus_info := &clus_info{
			server_address: strings.Split(*peers, ","),
		}
		fmt.Printf("%v\n", clus_info.server_address)
		_, close_peer := curp.RunStateMachine(clus_info.server_address, *grpc_addr, *id+1)
		defer close_peer()
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	fmt.Println("\r- Ctrl+C pressed in Terminal")
	// os.Exit(0)
}
