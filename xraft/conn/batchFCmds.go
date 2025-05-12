package conn

import (
	"github/Fischer0522/xraft/xraft/pb"
	"sync"
)

type batchedFCmds struct {
	mu           sync.Mutex
	batchRequest *pb.FBFCmd
	block        bool
}

func (c *batchedFCmds) startbatch(FTerm uint32) {
	c.batchRequest.FTerm = FTerm
	c.batchRequest.Cmds = make([]*pb.Request, 0, 32)
	c.block = false
}

func (c *batchedFCmds) batch(r *pb.Request) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.block {
		return false
	}
	c.batchRequest.Cmds = append(c.batchRequest.Cmds, r)
	return true
}

func (c *batchedFCmds) down() *pb.FBFCmd {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.block = true
	return c.batchRequest
}
