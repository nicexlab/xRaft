package conn

import (
	"github/Fischer0522/xraft/kv"
	"github/Fischer0522/xraft/xraft/pb"
	"github/Fischer0522/xraft/xraft/request"
	"sync"
	"testing"
)

func TestSlow(t *testing.T) {
	kv, err := kv.NewBoltStore("test.db", "test")
	defer kv.Destroy()
	if err != nil {
		t.Error(err)
	}
	p, _, _ := NewParticipants_2PC(0, kv, 0, nil)

	r := &pb.Request{SeqId: 11, ClientID: 2, Command: &pb.Command{Op: pb.PUT, Key: "key", Val: "val"}}
	p.ExecuteSlowCommand(r)

	p.AbortReq(request.RequestID{ClientID: 2, SeqID: 11}, 0)

	r2 := &pb.Request{SeqId: 12, ClientID: 2, Command: &pb.Command{Op: pb.GET, Key: "key"}}
	p.ExecuteSlowCommand(r2)

	rr := p.AbortReq(request.RequestID{ClientID: 2, SeqID: 12}, 0)
	if rr.Val != "val" {
		t.Errorf("get val error %v", rr.Val)
	}
}

func TestFastConflict(t *testing.T) {
	kv, err := kv.NewBoltStore("test.db", "test")
	defer kv.Destroy()
	if err != nil {
		t.Error(err)
	}
	p, _, _ := NewParticipants_2PC(0, kv, 0, nil)

	r := &pb.Request{SeqId: 11, ClientID: 2, Command: &pb.Command{Op: pb.PUT, Key: "key", Val: "val"}}
	mr := p.Propose(&pb.Message{Request: r})

	if mr.RR.ReqReply != uint32(pb.PREPARE_SUCCEED) {
		t.Errorf("should succeed")
	}

	r2 := &pb.Request{SeqId: 12, ClientID: 2, Command: &pb.Command{Op: pb.GET, Key: "key"}}
	mr2 := p.Propose(&pb.Message{Request: r2})

	if mr2.RR.ReqReply != uint32(pb.PREPARE_CONFLICT) {
		t.Errorf("should conflict")
	}
	if len(mr2.RR.ConflictReqs) == 0 {
		t.Errorf("have ConflictReqs")
	}

	r3 := &pb.Request{SeqId: 13, ClientID: 2, Command: &pb.Command{Op: pb.PUT, Key: "key", Val: "val2"}}
	mr3 := p.Propose(&pb.Message{Request: r3, CommitCmd: 12})

	if mr3.RR.ReqReply != uint32(pb.PREPARE_CONFLICT) {
		t.Errorf("should conflict")
	}
	if len(mr3.RR.ConflictReqs) != 1 {
		t.Errorf("error ConflictReqs %v", mr3.RR.ConflictReqs)
	}
}

func TestMergeInfo(t *testing.T) {
	kv, err := kv.NewBoltStore("test.db", "test")
	defer kv.Destroy()
	if err != nil {
		t.Error(err)
	}
	p, MC, _ := NewParticipants_2PC(0, kv, 0, nil)

	r := &pb.Request{SeqId: 1, ClientID: 1, Command: &pb.Command{Op: pb.PUT, Key: "key", Val: "val"}}
	mr := p.Propose(&pb.Message{Request: r})

	if mr.RR.ReqReply != uint32(pb.PREPARE_SUCCEED) {
		t.Errorf("should succeed")
	}

	r2 := &pb.Request{SeqId: 1, ClientID: 2, Command: &pb.Command{Op: pb.PUT, Key: "key", Val: "val"}}
	mr2 := p.Propose(&pb.Message{Request: r2})

	if mr2.RR.ReqReply != uint32(pb.PREPARE_CONFLICT) {
		t.Errorf("should conflict")
	}

	// commit 1,1
	p.Propose(&pb.Message{CommitCmd: 1, ClientID: 1})

	r3 := &pb.Request{SeqId: 1, ClientID: 3, Command: &pb.Command{Op: pb.PUT, Key: "key", Val: "val"}}
	mr3 := p.Propose(&pb.Message{Request: r3})
	if mr3.RR.ReqReply != uint32(pb.PREPARE_CONFLICT) {
		t.Errorf("should conflict")
	}

	go func() {
		// client 3 start a merge to sync the result
		p.AbortReq(request.RequestID{ClientID: 3, SeqID: 1}, 0)
	}()
	mrInfo := <-MC

	if mrInfo.Start != 1 {
		t.Errorf("should start from 1")
	}
	if len(mrInfo.Reqs) != 2 {
		t.Errorf("should merge two %v", mrInfo.Reqs)
	}
}

func TestSendFastRetSlow(t *testing.T) {
	kv, err := kv.NewBoltStore("test.db", "test")
	defer kv.Destroy()
	if err != nil {
		t.Error(err)
	}
	p, MC, SC := NewParticipants_2PC(0, kv, 0, nil)

	r3 := &pb.Request{SeqId: 1, ClientID: 1, Command: &pb.Command{Op: pb.PUT, Key: "key", Val: "val"}}
	p.Propose(&pb.Message{Request: r3})

	p.BecomeLeader()
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		// client 3 start a merge to sync the result
		p.AbortReq(request.RequestID{ClientID: 1, SeqID: 1}, 0)
		wg.Done()
	}()
	mrInfo := <-MC
	r4 := &pb.Request{SeqId: 1, ClientID: 2, Command: &pb.Command{Op: pb.PUT, Key: "key", Val: "val"}}
	go func() {
		mr4 := p.Propose(&pb.Message{Request: r4})
		if mr4.RR.ReqReply != uint32(pb.SLOW_SUCCEED) {
			t.Errorf("ReqReply not equal SLOW_SUCCEED %v", mr4.String())
		}
		wg.Done()
	}()
	slc := <-SC
	p.SetToSlow(mrInfo)
	p.ExecuteSlowCommand(slc)
	wg.Wait()
}
