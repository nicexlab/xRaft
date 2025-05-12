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

type clus_info struct {
	server_address     []string
	grpcserver_address []string
}

var (
	runcluster = flag.Bool("s", false, "start a single peer or the cluster?")
	peers      = flag.String("addrs", "http://127.0.0.1:7010,http://127.0.0.1:7011,http://127.0.0.1:7012", "address of raftexample")
	grpc_peers = flag.String("gaddrs", ":8020,:8021,:8022", "address of grpc server")

	id = flag.Int("id", 0, "id of the replica")

	grpc_addr = flag.String("gport", ":11041", "port of the grpc server")

	open_batch = flag.Bool("b", false, "batch the cmds when server is blocked to fast")
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

// go run main.go -addrs http://192.168.0.203:7010,http://192.168.0.204:7010,http://192.168.0.206:7010 -id
// 开启batch的运行： go run main.go -b -addrs http://192.168.0.203:7010,http://192.168.0.204:7010,http://192.168.0.206:7010 -gport :11041,:11041,:11041 -id 0
func main() {
	flag.Parse()

	self_gports := *grpc_addr

	addrs := strings.Split(*peers, ",")
	servers_gaddrs := make([]string, len(addrs))
	if *open_batch {
		gports := strings.Split(*grpc_addr, ",")
		if len(gports) != len(addrs) {
			log.Fatalf("Error open batch, must provide grpc port for each server")
		}
		for i := range addrs {
			url, err := url.Parse(addrs[i])
			if err != nil {
				fmt.Println("Error parsing URL:", err)
				return
			}
			servers_gaddrs[i] = fmt.Sprintf("%s%s", url.Hostname(), gports[i])
		}
		self_gports = gports[*id]
	}

	if !*runcluster { // 默认配置在本地启动
		clus_info := &clus_info{
			server_address:     strings.Split(*peers, ","),
			grpcserver_address: strings.Split(*grpc_peers, ","),
		}
		number := len(clus_info.grpcserver_address)
		if number != len(clus_info.server_address) {
			log.Fatal("flag error")
		}
		states := make([]*xraft.StateMachine, number)
		for i := range states {
			if *open_batch {
				stat, close_peer := xraft.RunStateMachine(clus_info.server_address, clus_info.grpcserver_address[i], i+1, clus_info.grpcserver_address)
				states[i] = stat
				defer close_peer()
			} else {
				stat, close_peer := xraft.RunStateMachine(clus_info.server_address, clus_info.grpcserver_address[i], i+1, nil)
				states[i] = stat
				defer close_peer()
			}
		}
	} else {
		clus_info := &clus_info{
			server_address: strings.Split(*peers, ","),
		}
		fmt.Printf("%v\n", clus_info.server_address)
		fmt.Printf("%v\n", servers_gaddrs)
		if *open_batch {
			_, close_peer := xraft.RunStateMachine(clus_info.server_address, self_gports, *id+1, servers_gaddrs)
			defer close_peer()
		} else {
			_, close_peer := xraft.RunStateMachine(clus_info.server_address, self_gports, *id+1, nil)
			defer close_peer()
		}
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-c
	fmt.Println("\r- Ctrl+C pressed in Terminal")
	// os.Exit(0)
}
