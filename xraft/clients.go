package xraft

import (
	"fmt"
	"github/Fischer0522/xraft/xraft/conn"
	"github/Fischer0522/xraft/xraft/pb"
	"sync/atomic"
)

// import (
// 	"github/Fischer0522/xraft/xraft/conn"
// 	"github/Fischer0522/xraft/xraft/proto"
// 	"math/rand"
// 	"sync/atomic"
// 	"time"
// )

type proxy interface {
	Submit(R *pb.Request) string
	Static() string
}

type Client struct {
	proxy          proxy
	seqId          uint64
	pingfs         []func() int64
	pingAndWritefs []func(int) int64
}

func (c *Client) Static() {
	fmt.Printf(c.proxy.Static())
}

func newClient(peers []*conn.Participants_2PC, id int) *Client {
	c := &Client{
		proxy: conn.Init_Coordinator_2PC(peers, id),
	}
	return c
}

func NewGrpcClient(peers []string, id int) *Client {
	coor, ping_funcs, pingAndWrite_funcs := conn.Init_Coordinator_Grpc(peers, id)
	c := &Client{
		proxy:          coor,
		pingfs:         ping_funcs,
		pingAndWritefs: pingAndWrite_funcs,
	}
	return c
}

func (c *Client) PingTest(times int) {

	for j := range c.pingfs {
		total := 0
		for i := 0; i < times; i++ {
			time := c.pingfs[j]()
			total += int(time)
			fmt.Printf("%v: ping time %vms\n", i+1, time)
		}
		fmt.Printf("Average ping time %v ms\n", total/times)
	}
}
func (c *Client) PingAndWriteTest(times int, size int) {

	for j := range c.pingfs {
		total := 0
		for i := 0; i < times; i++ {
			time := c.pingAndWritefs[j](size)
			total += int(time)
			fmt.Printf("%v: ping time %vms\n", i+1, time)
		}
		fmt.Printf("Average ping time %v ms\n", total/times)
	}
}

func (c *Client) Get(key string) string {

	cmd := &pb.Command{
		Op:  pb.GET,
		Key: key,
	}

	req := &pb.Request{
		SeqId:   atomic.AddUint64(&c.seqId, 1),
		Command: cmd,
	}
	return c.proxy.Submit(req)
}

func (c *Client) Set(key string, val string) string {
	cmd := &pb.Command{
		Op:  pb.PUT,
		Key: key,
		Val: val,
	}

	req := &pb.Request{
		SeqId:   atomic.AddUint64(&c.seqId, 1),
		Command: cmd,
	}
	return c.proxy.Submit(req)
}

func (c *Client) Del(key string) string {
	cmd := &pb.Command{
		Op:  pb.DELETE,
		Key: key,
	}

	req := &pb.Request{
		SeqId:   atomic.AddUint64(&c.seqId, 1),
		Command: cmd,
	}
	return c.proxy.Submit(req)
}

// func (c *Client) Close() {
// 	c.proxy.Close()
// }
