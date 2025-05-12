package main

import (
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"github/Fischer0522/xraft/xraft"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
)

type clus_info struct {
	server_address     []string
	grpcserver_address []string
}

var client_nums int
var id int

func create_pprof() *os.File {
	f, _ := os.OpenFile("client_cpu.pprof", os.O_CREATE|os.O_RDWR, 0644)
	pprof.StartCPUProfile(f)

	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)
	return f
}

func stop_pprof(f *os.File) {
	pprof.StopCPUProfile()
	f.Close()
	// 在程序结束前，将profile结果写入文件
	f1, err := os.Create("client_block.pprof")
	if err != nil {
		panic(err)
	}
	defer f1.Close()
	pprof.Lookup("block").WriteTo(f1, 0)

	if f2, err := os.Create("client_mutex.pprof"); err == nil {
		defer f2.Close()
		pprof.Lookup("mutex").WriteTo(f2, 0)
	}
}

func main() {
	flag.IntVar(&client_nums, "n", 1, "client_nums")
	flag.IntVar(&id, "id", 0, "client id, start from 0")
	flag.Parse()
	clus_info := &clus_info{
		grpcserver_address: []string{"192.168.0.203:11041", "192.168.0.204:11041", "192.168.0.206:11041"},
		// grpcserver_address: []string{":8020", ":8021", ":8022"},
	}

	if client_nums != 1 {
		clients := make([]*xraft.Client, client_nums)
		defer func() {
			for i := range clients {
				fmt.Printf("client %v:", i)
				clients[i].Static()
			}
		}()

		for i := range clients {
			clients[i] = xraft.NewGrpcClient(clus_info.grpcserver_address, i)
			go bench_server(fmt.Sprintf(":%v", 9360+i), clients[i])
		}
	} else {

		client := xraft.NewGrpcClient(clus_info.grpcserver_address, id)
		go bench_server(fmt.Sprintf(":%v", 9360+id), client)
		defer func() {
			fmt.Printf("client %v:", id)
			client.Static()
		}()
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	fmt.Println("\r- Ctrl+C pressed in Terminal")

}

type GetResponse struct {
	Count int64
	Kvs   []*Command
}

func (r *GetResponse) Encode() []byte {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	err := enc.Encode(r)
	if err != nil {
		fmt.Println("encode error:", err)
	}
	return buf.Bytes()
}

const (
	GET uint8 = iota
	PUT
	DELETE
)

type Command struct {
	Op    uint8
	Key   string
	Value string
}

func bench_server(port string, client *xraft.Client) {
	// 监听在本地端口9360
	listener, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// 函数退出时关闭监听器
	defer listener.Close()
	fmt.Printf("Server is listening on :%v\n", port)

	for {
		// 等待连接
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// 开启一个goroutine处理连接
		go handleRequest(conn, client)
	}
}

// 处理请求
func handleRequest(conn net.Conn, client *xraft.Client) {

	buf := make([]byte, 4096)
	n, _ := conn.Read(buf)

	aCmd := &Command{}
	dec := gob.NewDecoder(bytes.NewReader(buf[:n]))
	err := dec.Decode(aCmd)
	if err != nil {
		fmt.Println(err)
		return
	}
	// fmt.Printf("Received: %+v\n", aCmd)

	if aCmd.Op == GET {
		// call client.get
		key := aCmd.Key
		value := client.Get(aCmd.Key)
		cmd := Command{
			Op:    GET,
			Key:   key,
			Value: value,
		}
		cmds := []*Command{&cmd}
		resp := GetResponse{
			Count: 1,
			Kvs:   cmds,
		}
		buf := resp.Encode()
		conn.Write(buf)
	} else if aCmd.Op == PUT {
		// call client.put

		client.Set(aCmd.Key, aCmd.Value)

		conn.Write([]byte("Received PUT Response!"))
	} else if aCmd.Op == DELETE {
		// call client.delete
		client.Del(aCmd.Key)
		conn.Write([]byte("Received!"))
	}
	// send response to client
	conn.Write([]byte("Received DELETE Response!"))

	// 使用 Command
}
