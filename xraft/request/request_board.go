package request

import (
	"github/Fischer0522/xraft/xraft/pb"
	"sync"
)

type RequestID struct {
	ClientID uint64
	SeqID    uint64
}

func GetRequestID(req *pb.Request) RequestID {
	return RequestID{ClientID: req.ClientID, SeqID: req.SeqId}
}

type RequestBoard struct {
	mu sync.Mutex

	// store all notifiers for merge/slow results
	mrNotifiers map[RequestID]chan any

	mrBuffer map[RequestID]any
}

func NewBoard() *RequestBoard {
	return &RequestBoard{
		mrNotifiers: make(map[RequestID]chan any),
		mrBuffer:    make(map[RequestID]any),
		mu:          sync.Mutex{},
	}
}

func (r *RequestBoard) InsertMr(id RequestID, result any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mrBuffer[id] = result
	r.NotifyMr(id, result)
}

func (r *RequestBoard) NotifyMr(id RequestID, result any) {
	if _, ok := r.mrNotifiers[id]; ok {
		r.mrNotifiers[id] <- result
	}
}

func (r *RequestBoard) WaitForMr(id RequestID) any {
	r.mu.Lock()
	if result, ok := r.mrBuffer[id]; ok {
		// TODO: remove result from buffer will cause an error that slow path will lose the result
		// do we have a better to do garbage collection for buffer?
		//delete(c.erBuffer, id)
		r.mu.Unlock()
		return result
	}
	if _, ok := r.mrNotifiers[id]; !ok {
		r.mrNotifiers[id] = make(chan any, 1)
	}
	notifyChan := r.mrNotifiers[id]
	r.mu.Unlock()
	result := <-notifyChan
	return result
}

func (r *RequestBoard) Clear() {
	r.mu.Lock()
	r.mrBuffer = make(map[RequestID]any)
	r.mrNotifiers = make(map[RequestID]chan any)
	r.mu.Unlock()
}

func (r *RequestBoard) ClearID(id RequestID) {
	r.mu.Lock()
	delete(r.mrBuffer, id)
	delete(r.mrNotifiers, id)
	r.mu.Unlock()
}
