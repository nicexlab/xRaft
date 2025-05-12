package conn

import (
	"fmt"
	xlog "github/Fischer0522/xraft/xraft/log"
	"github/Fischer0522/xraft/xraft/pb"
	"github/Fischer0522/xraft/xraft/request"
	"time"

	"go.uber.org/zap"
)

type xraftServer interface {
	AbortReq(cmdID request.RequestID, FTerm uint32) (reply *pb.RequestReply)
	Propose(mess *pb.Message) *pb.MessageReply
}

type Static struct {
	fastPath      int
	slowPath      int
	conflict      int
	batched_block int
	resend_time   int
}

func (s *Static) String() string {
	return fmt.Sprintf("fastpath: %v, slowpath %v, conflict %v, processed in batched %v, blocked times %v\n", s.fastPath, s.slowPath, s.conflict, s.batched_block, s.resend_time)
}

type roundResult uint8

const (
	fast_succeed roundResult = iota
	fast_resend
	fast_conflict // 副本检测到冲突， 需要考虑进行slow 或者 commit
	slow_succeed  // 在slow中被处理了
	batched_process_when_blocked

	time_out
	round_error
)

type PeerReply struct {
	reply  *pb.MessageReply
	peerid int
}

// Client-side communication proxy
type Coordinator struct { // 这个结构负责client向peers的2PC过程
	peerNum  int
	peers    []xraftServer
	ClientID uint64
	// txsCache_pre    map[uint64]*register // 暂存 prepare的tx
	has_commit   bool
	commmit_info uint64 // 暂存 上一个request中 commit的cmd
	// txsCache_abo    map[uint64]*register // 暂存 abo的tx
	// txsCache_slo    map[uint64]*register
	logger *zap.SugaredLogger
	mSeqId uint64
	static Static

	// mu sync.Mutex
}

func (co *Coordinator) Static() string {
	return co.static.String()
}

func Init_Coordinator(servers []xraftServer, id int) *Coordinator {
	co := &Coordinator{
		peerNum: len(servers),
		peers:   servers,

		mSeqId: 0,
		logger: xlog.InitLogger().Named(fmt.Sprintf("client-%v", id)),
		static: Static{fastPath: 0, slowPath: 0, conflict: 0},
		// mu:       sync.Mutex{},
		ClientID: uint64(id),
	}

	return co
}

func (co *Coordinator) WarpFastRequest(R *pb.Request) *pb.Message {
	mess := &pb.Message{}
	mess.Request = R

	// co.mu.Lock()
	mess.MSeqId = co.mSeqId
	co.mSeqId++
	if co.has_commit {
		mess.CommitCmd = co.commmit_info
		co.has_commit = false
	}
	// co.mu.Unlock()
	mess.ClientID = uint64(co.ClientID)
	mess.Request.ClientID = uint64(co.ClientID)
	return mess
}

func (co *Coordinator) WarpSlowRequest(R *pb.Request) *pb.Message {
	mess := &pb.Message{}
	mess.Request = R

	// co.mu.Lock()
	mess.MSeqId = co.mSeqId
	co.mSeqId++
	// co.mu.Unlock()
	return mess
}

func (co *Coordinator) UpdateCommit(seqId uint64) {
	// co.mu.Lock()
	co.commmit_info = seqId
	co.has_commit = true
	// co.mu.Unlock()
}

func (co *Coordinator) Submit(R *pb.Request) string {
	var result string
	var state roundResult
	var leader int
	var fterm uint32
	for {
		mess := co.WarpFastRequest(R)
		result, state, leader, fterm = co.Round(mess)
		if state != fast_resend {
			break
		}
		co.static.resend_time++
	}

	if state == fast_succeed {
		co.UpdateCommit(R.SeqId)
		co.logger.Debugf("%v commit in fast", R.SeqId)
		co.static.fastPath++
		return result
	} else if state == slow_succeed { // 在slow中被处理了
		co.logger.Debugf("%v commit in slow", R.SeqId)
		co.static.slowPath++
		return result
	} else if state == fast_conflict {
		co.static.conflict++
		co.logger.Debugf("abort a req %v", R.SeqId)
		rr := co.peers[leader].AbortReq(request.RequestID{SeqID: R.SeqId, ClientID: R.ClientID}, fterm)
		result = rr.Val
	} else if state == batched_process_when_blocked {
		co.static.batched_block++
		co.logger.Debugf("commit in batch %v", R.SeqId)
		return result
	}

	return result
}

func (co *Coordinator) Round(mess *pb.Message) (string, roundResult, int, uint32) {
	fastPathChan := make(chan *PeerReply, 5)
	timeout := time.NewTicker(5 * time.Second)

	for i := 0; i < co.peerNum; i++ {
		go func(id int) {
			rr := co.peers[id].Propose(mess)
			fastPathChan <- &PeerReply{reply: rr, peerid: id}
		}(i)
	}

	receiveCount := 0
	replies := make([]*pb.MessageReply, co.peerNum)

	for {
		select {
		case fastResult := <-fastPathChan:
			receiveCount++
			replies[fastResult.peerid] = fastResult.reply
			if receiveCount == co.peerNum {
				return co.processFastResult(replies)
			}
		case <-timeout.C:
			fmt.Printf("timeout !!\n")
			return "", time_out, -1, 0
		}
	}
}

func reqIDEqual(id1 *pb.RequestID, id2 *pb.RequestID) bool {
	return id1.ClientID == id2.ClientID && id1.SeqId == id2.SeqId
}

func (co *Coordinator) processFastResult(replies []*pb.MessageReply) (string, roundResult, int, uint32) {
	all_accept := true
	all_fast := true

	same_conflicts := true
	same_fterm := true

	var FTerm uint32 = 0

	all_fast_no_conflict := true

	for i := range replies {
		if replies[i].RR.ReqReply != uint32(pb.PREPARE_SUCCEED) {
			all_accept = false
			if replies[i].RR.ReqReply != uint32(pb.PREPARE_CONFLICT) {
				all_fast = false
			} else { // 有Prepare_conflict发生
				all_fast_no_conflict = false
			}
		}
	}

	FTerm = replies[0].RR.Fterm
	for i := 1; i < len(replies); i++ {
		if replies[i].RR.Fterm != FTerm {
			same_fterm = false
			break
		}
	}

	for i := 1; i < len(replies); i++ {
		if len(replies[i].RR.ConflictReqs) != len(replies[0].RR.ConflictReqs) {
			same_conflicts = false
			break
		}
		for j := range replies[i].RR.ConflictReqs {
			if !reqIDEqual(replies[i].RR.ConflictReqs[j], replies[0].RR.ConflictReqs[j]) {
				same_conflicts = false
				break
			}
		}
		if !same_conflicts {
			break
		}
	}

	if all_accept {
		if same_fterm {
			return replies[0].RR.Val, fast_succeed, 0, FTerm // 快速路径无冲突成功
		} else {
			return "", fast_resend, 0, FTerm // 全部接受但是有部分副本以错误的fast term接受时需要进行重发
		}
	}

	if all_fast && same_conflicts && same_fterm { // 当全部处于快速模式，并且以相同的term返回冲突时可以直接commit这个req
		return replies[0].RR.Val, fast_succeed, 0, FTerm
	}

	leader := -1
	for i := range replies {
		if replies[i].Leader != 0 {
			if leader != -1 {
				co.logger.Warnf("more than one replica is leader: %v and %v", leader, i)
			}
			leader = i
		}
	}

	switch replies[leader].RR.ReqReply {
	case uint32(pb.CURRENT_PREAPRE_FAST): // 当前正在准备快速模式， client重发这个请求
		return "", fast_resend, leader, FTerm
	case uint32(pb.PREPARE_SUCCEED):
		if !all_fast && all_fast_no_conflict { // 当不是所有的副本都处于Fast 状态时，如果所有的fast模式下的副本都接受了这个请求，重发这个请求
			return "", fast_resend, leader, FTerm
		}
	case uint32(pb.SLOW_SUCCEED):
		return replies[leader].RR.Val, slow_succeed, leader, FTerm
	case uint32(pb.BATCHED_FAST_SUCCEED):
		return replies[leader].RR.Val, batched_process_when_blocked, leader, FTerm
	}

	return "", fast_conflict, leader, FTerm
}
