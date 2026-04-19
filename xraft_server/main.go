package main

import (
	"flag"
	"fmt"
	"github/Fischer0522/xraft/xraft"
	"log"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"
)

type clusterInfo struct {
	serverAddress     []string
	grpcServerAddress []string
}

var (
	runCluster = flag.Bool("s", false, "start a single peer or the cluster?")
	peers      = flag.String("addrs", "http://127.0.0.1:7010,http://127.0.0.1:7011,http://127.0.0.1:7012", "address of raftexample")
	grpcPeers  = flag.String("gaddrs", ":8020,:8021,:8022", "address of grpc server")

	id = flag.Int("id", 0, "id of the replica")

	grpcAddr = flag.String("gport", ":11041", "port of the grpc server")

	enableBatch = flag.Bool("b", false, "batch the cmds when server is blocked to fast")
)

func create_pprof() *os.File {
	f, _ := os.OpenFile("server_cpu.pprof", os.O_CREATE|os.O_RDWR, 0644)
	pprof.StartCPUProfile(f)

	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)
	return f
}

func stop_pprof(f *os.File) {
	pprof.StopCPUProfile()
	f.Close()
	f1, err := os.Create("server_block.pprof")
	if err != nil {
		panic(err)
	}
	defer f1.Close()
	pprof.Lookup("block").WriteTo(f1, 0)

	if f2, err := os.Create("server_mutex.pprof"); err == nil {
		defer f2.Close()
		pprof.Lookup("mutex").WriteTo(f2, 0)
	}
}

func buildBatchGRPCAddrs(peerAddrs []string, grpcPorts []string) ([]string, error) {
	if len(grpcPorts) != len(peerAddrs) {
		return nil, fmt.Errorf("batch mode requires one grpc port for each raft peer")
	}

	serverGRPCAddrs := make([]string, len(peerAddrs))
	for i := range peerAddrs {
		peerURL, err := url.Parse(peerAddrs[i])
		if err != nil {
			return nil, fmt.Errorf("parse peer URL %q: %w", peerAddrs[i], err)
		}
		serverGRPCAddrs[i] = fmt.Sprintf("%s%s", peerURL.Hostname(), grpcPorts[i])
	}
	return serverGRPCAddrs, nil
}

// go run main.go -addrs http://192.168.0.203:7010,http://192.168.0.204:7010,http://192.168.0.206:7010 -id
// 开启batch的运行： go run main.go -b -addrs http://192.168.0.203:7010,http://192.168.0.204:7010,http://192.168.0.206:7010 -gport :11041,:11041,:11041 -id 0
func main() {
	flag.Parse()

	selfGRPCPort := *grpcAddr

	addrs := strings.Split(*peers, ",")
	var serverGRPCAddrs []string
	if *enableBatch {
		gports := strings.Split(*grpcAddr, ",")
		if *id < 0 || *id >= len(gports) {
			log.Fatalf("replica id %d is outside grpc port list of length %d", *id, len(gports))
		}
		var err error
		serverGRPCAddrs, err = buildBatchGRPCAddrs(addrs, gports)
		if err != nil {
			log.Fatal(err)
		}
		selfGRPCPort = gports[*id]
	}

	if !*runCluster { // 默认配置在本地启动
		cluster := &clusterInfo{
			serverAddress:     strings.Split(*peers, ","),
			grpcServerAddress: strings.Split(*grpcPeers, ","),
		}
		number := len(cluster.grpcServerAddress)
		if number != len(cluster.serverAddress) {
			log.Fatal("flag error")
		}
		var fastBatchAddrs []string
		if *enableBatch {
			fastBatchAddrs = cluster.grpcServerAddress
		}
		states := make([]*xraft.StateMachine, number)
		for i := range states {
			stat, closePeer := xraft.RunStateMachine(cluster.serverAddress, cluster.grpcServerAddress[i], i+1, fastBatchAddrs)
			states[i] = stat
			defer closePeer()
		}
	} else {
		cluster := &clusterInfo{
			serverAddress: strings.Split(*peers, ","),
		}
		fmt.Printf("%v\n", cluster.serverAddress)
		fmt.Printf("%v\n", serverGRPCAddrs)
		_, closePeer := xraft.RunStateMachine(cluster.serverAddress, selfGRPCPort, *id+1, serverGRPCAddrs)
		defer closePeer()
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-c
	fmt.Println("\r- Ctrl+C pressed in Terminal")
	// os.Exit(0)
}
