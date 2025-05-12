package curp

import (
	"fmt"
	"github/Fischer0522/xraft/curp/command"
	"github/Fischer0522/xraft/curp/curp_proto"
	"github/Fischer0522/xraft/curp/trace"
	"math"
	"sync/atomic"
	"time"
)

type MockClient struct {
	Servers  []*StateMachine
	ClientId uint64
	SeqId    uint64
	f        int
	LeaderId int
}

func NewClient(servers []*StateMachine, clientId uint64) (*MockClient, error) {
	// 2f + 1 = len(servers)
	f := (len(servers) - 1) / 2
	c := MockClient{
		Servers:  servers,
		ClientId: clientId,
		SeqId:    0,
		f:        f,
	}
	c.LeaderId = c.fetchLeader()
	if c.LeaderId == -1 {
		return nil, fmt.Errorf("no leader")
	}
	return &c, nil
}

func (c *MockClient) fetchLeader() int {
	leaderId := -1
	for idx, server := range c.Servers {
		if server.IsLeader() {
			leaderId = idx
		}
	}
	return leaderId
}

func (c *MockClient) GetProposeId() command.ProposeId {
	newSeqId := atomic.AddUint64(&c.SeqId, 1)

	return command.ProposeId{
		SeqId:    newSeqId,
		ClientId: c.ClientId,
	}
}

// 2f + 1 replicas, f + f / 2 + 1
func (c *MockClient) superQuorom() int {
	return c.f + int(math.Ceil(float64(c.f)/2)) + 1
}

func (c *MockClient) Put(key string, value string) error {

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

func (c *MockClient) Get(key string) (string, error) {
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

func (c *MockClient) Propose(cmd *curp_proto.CurpClientCommand) (string, error) {
	reply, err := c.fastRound(cmd)
	if err != nil {
		return "", err
	}

	if reply.StatusCode == curp_proto.ACCEPTED {
		return reply.Content, nil
	}
	trace.Trace(trace.Client, -1, "client receive conflict result, turn to slow path: %v", reply)
	resultStr, err := c.slowRound(cmd)
	return resultStr, err
}

func (c *MockClient) fastRound(cmd *curp_proto.CurpClientCommand) (*curp_proto.CurpReply, error) {

	timeout := time.NewTicker(200 * time.Millisecond)
	fastPathChan := make(chan *curp_proto.CurpReply)
	for i := 0; i < len(c.Servers); i++ {
		go func(i int) {

			result := c.Servers[i].Propose(cmd)
			fastPathChan <- result
		}(i)
	}
	acceptedCount := 0

	result := &curp_proto.CurpReply{
		StatusCode: curp_proto.TIMEOUT,
		Content:    "",
	}

	for {
		select {
		case fastResult := <-fastPathChan:
			trace.Trace(trace.Client, -1, "client receive fast path result: %v", fastResult)
			if fastResult.StatusCode == curp_proto.ACCEPTED {
				// if acceptedCount > 0 && reply != fastResult.Content {
				// 	return fastResult, fmt.Errorf("reply result mismatch,prev:%s,now:%d", reply, fastResult.StatusCode)
				// }
				if result.Content != "" {
					result = fastResult
				}
				acceptedCount++
			} else {
				return fastResult, nil
			}

			if acceptedCount == c.superQuorom() {
				return result, nil
			}
		case <-timeout.C:
			fmt.Printf("timeout !!\n")
			return result, fmt.Errorf("fast path time out")
		}
	}

}

func (c *MockClient) slowRound(cmd *curp_proto.CurpClientCommand) (string, error) {
	leader := c.Servers[c.LeaderId]

	reply := leader.WaitSynced(cmd.ProposeId())
	trace.Trace(trace.Client, -1, "client wait synced finished, id: %v", cmd.ProposeId)
	if reply.StatusCode == curp_proto.CONFLICT {
		return "", fmt.Errorf("command is still conflict after wait synced")
	}
	return reply.Content, nil
}
