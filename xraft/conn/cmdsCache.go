package conn

import (
	"github/Fischer0522/xraft/xraft/request"
	"sync"
)

type cmdsCache struct {
	cache map[request.RequestID]*ResultedCommand
	mu    sync.Mutex
}

func newCmdsCache() *cmdsCache {
	return &cmdsCache{
		cache: make(map[request.RequestID]*ResultedCommand),
		mu:    sync.Mutex{},
	}
}

func (c *cmdsCache) insert(cmdID request.RequestID, res *ResultedCommand) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache[cmdID] = res
}

func (c *cmdsCache) delete(cmdID request.RequestID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cache, cmdID)
}

func (c *cmdsCache) batchDelete(cmdIDs []request.RequestID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cmdID := range cmdIDs {
		delete(c.cache, cmdID)
	}
}

func (c *cmdsCache) commit(cmdID request.RequestID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.cache[cmdID]; ok {
		c.cache[cmdID].committed = true

		delete(c.cache, cmdID)
	}
}

func (c *cmdsCache) get(cmdID request.RequestID) (*ResultedCommand, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	res, ok := c.cache[cmdID]
	return res, ok
}
