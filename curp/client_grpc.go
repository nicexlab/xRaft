package curp

import (
	"context"
	"fmt"
	"github/Fischer0522/xraft/curp/command"
	"github/Fischer0522/xraft/curp/curp_proto"
	"github/Fischer0522/xraft/curp/trace"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type static struct {
	fast int
	slow int
}

func (s *static) String() string {
	return fmt.Sprintf("fast: %v, slow: %v", s.fast, s.slow)
}

type Grpc_client struct {
	grpc_servers []string
	client       []curp_proto.CurpClient
	conns        []*grpc.ClientConn
	ClientId     uint64
	SeqId        uint64
	f            int
	LeaderId     int

	mu     sync.RWMutex
	static static
}

func NewGrpcClient(servers []string, clientId uint64) (*Grpc_client, error) {
	// 2f + 1 = len(servers)
	f := (len(servers) - 1) / 2
	c := Grpc_client{
		grpc_servers: servers,
		client:       make([]curp_proto.CurpClient, len(servers)),
		conns:        make([]*grpc.ClientConn, len(servers)),
		ClientId:     clientId,
		SeqId:        0,
		f:            f,

		mu:     sync.RWMutex{},
		static: static{},
	}
	for i := range c.grpc_servers {
		conn, err := grpc.NewClient(c.grpc_servers[i], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("net.Connect err: %v", err)
		}
		c.conns[i] = conn
		c.client[i] = curp_proto.NewCurpClient(conn)
	}
	c.LeaderId = c.fetchLeader()
	if c.LeaderId == -1 {
		return nil, fmt.Errorf("no leader")
	}

	return &c, nil
}

func (c *Grpc_client) Static() string {
	return c.static.String()
}

func (c *Grpc_client) isLeader(peerId int) bool {
	// 调用Register注册客户信息
	r, err := c.client[peerId].IsLeader(context.Background(), &curp_proto.LeaderAck{})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}

	return r.IsLeader == 1
}

func (c *Grpc_client) fetchLeader() int {
	leaderId := -1
	for idx := range c.grpc_servers {
		if c.isLeader(idx) {
			leaderId = idx
		}
	}
	return leaderId
}

func (c *Grpc_client) GetProposeId() command.ProposeId {
	newSeqId := atomic.AddUint64(&c.SeqId, 1)

	return command.ProposeId{
		SeqId:    newSeqId,
		ClientId: c.ClientId,
	}
}

// 2f + 1 replicas, f + f / 2 + 1
func (c *Grpc_client) superQuorom() int {
	return c.f + int(math.Ceil(float64(c.f)/2)) + 1
}

func (c *Grpc_client) Put(key string, value string) error {

	proposeId := c.GetProposeId()
	cmd := &curp_proto.CurpClientCommand{
		Op:       command.PUT,
		Key:      key,
		Value:    value,
		ClientId: proposeId.ClientId,
		SeqId:    proposeId.SeqId,
	}

	_, err := c.Propose(cmd)
	return err
}

func (c *Grpc_client) Get(key string) (string, error) {

	proposeId := c.GetProposeId()
	cmd := &curp_proto.CurpClientCommand{
		Op:       command.GET,
		Key:      key,
		ClientId: c.ClientId,
		SeqId:    proposeId.SeqId,
	}
	result, err := c.Propose(cmd)

	return result, err
}

func (c *Grpc_client) Del(key string) error {

	proposeId := c.GetProposeId()
	cmd := &curp_proto.CurpClientCommand{
		Op:       command.DELETE,
		Key:      key,
		ClientId: c.ClientId,
		SeqId:    proposeId.SeqId,
	}
	_, err := c.Propose(cmd)

	return err
}

func (c *Grpc_client) Propose(cmd *curp_proto.CurpClientCommand) (string, error) {

	reply, err := c.fastRound(cmd)

	if err != nil {
		return "", err
	}

	if reply.StatusCode == curp_proto.ACCEPTED {
		c.static.fast++
		return reply.Content, nil
	}
	c.static.slow++
	trace.Trace(trace.Client, -1, "client receive conflict result, turn to slow path: %v", reply)
	log.Printf("CONFLICT: fast %d,slow: %d", c.static.fast, c.static.slow)
	resultStr, err := c.slowRound(cmd)

	return resultStr, err
}

type reply struct {
	peerId int
	result *curp_proto.CurpReply
}

func (c *Grpc_client) fastRound(cmd *curp_proto.CurpClientCommand) (*curp_proto.CurpReply, error) {
	// timeout := time.NewTicker(600 * time.Millisecond)
	timeout := time.NewTicker(time.Second * 5)
	fastPathChan := make(chan *reply)
	for i := 0; i < len(c.grpc_servers); i++ {
		go func(i int) {
			result, _ := c.client[i].Propose(context.Background(), cmd)

			// result := <-notifyC[i]
			fastPathChan <- &reply{peerId: i, result: result}
		}(i)
	}
	acceptedCount := 0

	result := &curp_proto.CurpReply{
		StatusCode: curp_proto.TIMEOUT,
		Content:    "",
	}

	results := make([]*curp_proto.CurpReply, len(c.grpc_servers))

	for {
		select {
		case fastResult := <-fastPathChan:
			trace.Trace(trace.Client, -1, "client receive fast path result: %v", fastResult)
			if fastResult.result.StatusCode == curp_proto.ACCEPTED {
				// if acceptedCount > 0 && reply != fastResult.Content {
				// 	return fastResult, fmt.Errorf("reply result mismatch,prev:%s,now:%d", reply, fastResult.StatusCode)
				// }
				acceptedCount++
				results[fastResult.peerId] = fastResult.result
			} else {
				return fastResult.result, nil
			}

			if acceptedCount == c.superQuorom() {
				return results[c.LeaderId], nil
			}
		case <-timeout.C:
			fmt.Printf("timeout !!\n")
			return result, fmt.Errorf("fast path time out")
		}
	}

}

func (c *Grpc_client) slowRound(cmd *curp_proto.CurpClientCommand) (string, error) {
	// leader := c.Servers[c.LeaderId]
	sync_cmd := &curp_proto.ProposeId{ClientId: cmd.ClientId, SeqId: cmd.SeqId}
	reply, _ := c.client[c.LeaderId].WaitSynced(context.Background(), sync_cmd)

	trace.Trace(trace.Client, -1, "client wait synced finished, id: %v", cmd.ProposeId)
	if reply.StatusCode == curp_proto.CONFLICT {
		return "", fmt.Errorf("command is still conflict after wait synced")
	}
	return reply.Content, nil
}
