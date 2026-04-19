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
	AbortReq(cmdID request.RequestID, fTerm uint32) (reply *pb.RequestReply)
	Propose(mess *pb.Message) *pb.MessageReply
}

type Static struct {
	fastPath     int
	slowPath     int
	conflict     int
	batchedBlock int
	resendTime   int
}

func (s *Static) String() string {
	return fmt.Sprintf("fastpath: %v, slowpath %v, conflict %v, processed in batched %v, blocked times %v\n", s.fastPath, s.slowPath, s.conflict, s.batchedBlock, s.resendTime)
}

type roundResult uint8

const (
	fastSucceed roundResult = iota
	fastResend
	fastConflict // 副本检测到冲突， 需要考虑进行slow 或者 commit
	slowSucceed  // 在slow中被处理了
	batchedProcessWhenBlocked

	timeoutResult
)

type peerReply struct {
	reply  *pb.MessageReply
	peerID int
}

// Client-side communication proxy
type Coordinator struct { // 这个结构负责client向peers的2PC过程
	peerNum  int
	peers    []xraftServer
	ClientID uint64
	// txsCache_pre    map[uint64]*register // 暂存 prepare的tx
	hasCommit  bool
	commitInfo uint64 // 暂存 上一个request中 commit的cmd
	// txsCache_abo    map[uint64]*register // 暂存 abo的tx
	// txsCache_slo    map[uint64]*register
	logger *zap.SugaredLogger
	mSeqID uint64
	static Static

	// mu sync.Mutex
}

func (co *Coordinator) Static() string {
	return co.static.String()
}

func newCoordinator(servers []xraftServer, id int) *Coordinator {
	co := &Coordinator{
		peerNum: len(servers),
		peers:   servers,

		mSeqID: 0,
		logger: xlog.InitLogger().Named(fmt.Sprintf("client-%v", id)),
		static: Static{fastPath: 0, slowPath: 0, conflict: 0},
		// mu:       sync.Mutex{},
		ClientID: uint64(id),
	}

	return co
}

func (co *Coordinator) wrapFastRequest(req *pb.Request) *pb.Message {
	mess := &pb.Message{}
	mess.Request = req

	// co.mu.Lock()
	mess.MSeqId = co.mSeqID
	co.mSeqID++
	if co.hasCommit {
		mess.CommitCmd = co.commitInfo
		co.hasCommit = false
	}
	// co.mu.Unlock()
	mess.ClientID = uint64(co.ClientID)
	mess.Request.ClientID = uint64(co.ClientID)
	return mess
}

func (co *Coordinator) wrapSlowRequest(req *pb.Request) *pb.Message {
	mess := &pb.Message{}
	mess.Request = req

	// co.mu.Lock()
	mess.MSeqId = co.mSeqID
	co.mSeqID++
	// co.mu.Unlock()
	return mess
}

func (co *Coordinator) updateCommit(seqID uint64) {
	// co.mu.Lock()
	co.commitInfo = seqID
	co.hasCommit = true
	// co.mu.Unlock()
}

func (co *Coordinator) Submit(req *pb.Request) string {
	var result string
	var state roundResult
	var leader int
	var fterm uint32
	for {
		mess := co.wrapFastRequest(req)
		result, state, leader, fterm = co.round(mess)
		if state != fastResend {
			break
		}
		co.static.resendTime++
	}

	if state == fastSucceed {
		co.updateCommit(req.SeqId)
		co.logger.Debugf("%v commit in fast", req.SeqId)
		co.static.fastPath++
		return result
	} else if state == slowSucceed { // 在slow中被处理了
		co.logger.Debugf("%v commit in slow", req.SeqId)
		co.static.slowPath++
		return result
	} else if state == fastConflict {
		co.static.conflict++
		co.logger.Debugf("abort a req %v", req.SeqId)
		rr := co.peers[leader].AbortReq(request.RequestID{SeqID: req.SeqId, ClientID: req.ClientID}, fterm)
		result = rr.Val
	} else if state == batchedProcessWhenBlocked {
		co.static.batchedBlock++
		co.logger.Debugf("commit in batch %v", req.SeqId)
		return result
	}

	return result
}

func (co *Coordinator) round(mess *pb.Message) (string, roundResult, int, uint32) {
	fastPathChan := make(chan *peerReply, co.peerNum)
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()

	for i := 0; i < co.peerNum; i++ {
		go func(id int) {
			rr := co.peers[id].Propose(mess)
			fastPathChan <- &peerReply{reply: rr, peerID: id}
		}(i)
	}

	receiveCount := 0
	replies := make([]*pb.MessageReply, co.peerNum)

	for {
		select {
		case fastResult := <-fastPathChan:
			receiveCount++
			replies[fastResult.peerID] = fastResult.reply
			if receiveCount == co.peerNum {
				return co.processFastResult(replies)
			}
		case <-timeout.C:
			fmt.Printf("timeout !!\n")
			return "", timeoutResult, -1, 0
		}
	}
}

func reqIDEqual(id1 *pb.RequestID, id2 *pb.RequestID) bool {
	return id1.ClientID == id2.ClientID && id1.SeqId == id2.SeqId
}

func (co *Coordinator) processFastResult(replies []*pb.MessageReply) (string, roundResult, int, uint32) {
	allAccept := true
	allFast := true

	sameConflicts := true
	sameFTerm := true

	var fTerm uint32

	allFastNoConflict := true

	for i := range replies {
		if replies[i].RR.ReqReply != uint32(pb.PREPARE_SUCCEED) {
			allAccept = false
			if replies[i].RR.ReqReply != uint32(pb.PREPARE_CONFLICT) {
				allFast = false
			} else { // 有Prepare_conflict发生
				allFastNoConflict = false
			}
		}
	}

	fTerm = replies[0].RR.Fterm
	for i := 1; i < len(replies); i++ {
		if replies[i].RR.Fterm != fTerm {
			sameFTerm = false
			break
		}
	}

	for i := 1; i < len(replies); i++ {
		if len(replies[i].RR.ConflictReqs) != len(replies[0].RR.ConflictReqs) {
			sameConflicts = false
			break
		}
		for j := range replies[i].RR.ConflictReqs {
			if !reqIDEqual(replies[i].RR.ConflictReqs[j], replies[0].RR.ConflictReqs[j]) {
				sameConflicts = false
				break
			}
		}
		if !sameConflicts {
			break
		}
	}

	if allAccept {
		if sameFTerm {
			return replies[0].RR.Val, fastSucceed, 0, fTerm // 快速路径无冲突成功
		} else {
			return "", fastResend, 0, fTerm // 全部接受但是有部分副本以错误的fast term接受时需要进行重发
		}
	}

	if allFast && sameConflicts && sameFTerm { // 当全部处于快速模式，并且以相同的term返回冲突时可以直接commit这个req
		return replies[0].RR.Val, fastSucceed, 0, fTerm
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
		return "", fastResend, leader, fTerm
	case uint32(pb.PREPARE_SUCCEED):
		if !allFast && allFastNoConflict { // 当不是所有的副本都处于Fast 状态时，如果所有的fast模式下的副本都接受了这个请求，重发这个请求
			return "", fastResend, leader, fTerm
		}
	case uint32(pb.SLOW_SUCCEED):
		return replies[leader].RR.Val, slowSucceed, leader, fTerm
	case uint32(pb.BATCHED_FAST_SUCCEED):
		return replies[leader].RR.Val, batchedProcessWhenBlocked, leader, fTerm
	}

	return "", fastConflict, leader, fTerm
}
