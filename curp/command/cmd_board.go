package command

import (
	"sync"
)

type CommandBoard struct {
	mu sync.Mutex
	// store all notifiers for execution results
	erNotifiers map[ProposeId]chan string
	// store all notifiers for after sync results
	asrNotifiers map[ProposeId]chan struct{}

	erBuffer map[ProposeId]string

	asrBuffer map[ProposeId]struct{}
}

func NewBoard() *CommandBoard {
	return &CommandBoard{
		erNotifiers:  make(map[ProposeId]chan string),
		asrNotifiers: make(map[ProposeId]chan struct{}),
		erBuffer:     make(map[ProposeId]string),
		asrBuffer:    make(map[ProposeId]struct{}),
	}
}

func (c *CommandBoard) InsertEr(id ProposeId, result string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.erBuffer[id] = result
	c.NotifyEr(id, result)
}

// func (c *CommandBoard) InsertAsr(id ProposeId, result string) {
// 	c.mu.Lock()
// 	defer c.mu.Unlock()
// 	c.asrBuffer[id] = result
// 	c.NotifyAsr(id, result)
// }

func (c *CommandBoard) NotifyEr(id ProposeId, result string) {

	if _, ok := c.erNotifiers[id]; ok {
		c.erNotifiers[id] <- result
	}
}

func (c *CommandBoard) NotifyAsr(id ProposeId) {
	c.mu.Lock()
	defer c.mu.Unlock()
	//trace.Trace(trace.Slow, "insert %v into asr buffer", id)
	c.asrBuffer[id] = struct{}{}
	if _, ok := c.asrNotifiers[id]; ok {
		c.asrNotifiers[id] <- struct{}{}
	}
}

func (c *CommandBoard) WaitForEr(id ProposeId) string {

	c.mu.Lock()
	if result, ok := c.erBuffer[id]; ok {
		// TODO: remove result from buffer will cause an error that slow path will lose the result
		// do we have a better to do garbage collection for buffer?
		//delete(c.erBuffer, id)
		c.mu.Unlock()
		return result
	}
	if _, ok := c.erNotifiers[id]; !ok {
		c.erNotifiers[id] = make(chan string, 1)
	}
	notifyChan := c.erNotifiers[id]
	c.mu.Unlock()
	result := <-notifyChan
	return result

}

// FIXME: how to handle result if wait synced failed?
func (c *CommandBoard) WaitForAsr(id ProposeId) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	//trace.Trace(trace.Client, "Wait Synced [clientId: %d,seqId: %d]", id.ClientId, id.SeqId)
	_, asrOk := c.asrBuffer[id]
	result, erOk := c.erBuffer[id]

	defer delete(c.asrBuffer, id)
	defer delete(c.erBuffer, id)

	// command has already been executed in both fast path and slow path
	if asrOk && erOk {
		return result
	}

	if _, ok := c.asrNotifiers[id]; !ok {
		c.asrNotifiers[id] = make(chan struct{}, 1)
	}
	notifyChan := c.asrNotifiers[id]
	// release lock and wait
	c.mu.Unlock()
	<-notifyChan
	c.mu.Lock()
	if result, ok := c.erBuffer[id]; ok {
		return result
	}
	// UNREACHABLE
	return "NOT FOUND IN BUFFER"
}

// func (c *CommandBoard) WaitForErAndAsr(id ProposeId) (string, string) {
// 	c.mu.Lock()
// 	er, erOk := c.er_buffer[id]
// 	asr, asrOk := c.asr_buffer[id]
// 	c.mu.Unlock()
// 	if erOk && asrOk {
// 		return er, asr
// 	} else if erOk && !asrOk {
// 		return er, ""
// 	}

// 	result := <-c.asr_notifiers[id]
// }
