package conn

import (
	"fmt"
	xkv "github/Fischer0522/xraft/kv"
	xlog "github/Fischer0522/xraft/xraft/log"
	"github/Fischer0522/xraft/xraft/pb"
	"github/Fischer0522/xraft/xraft/request"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

type MergeInfo struct { // 记录了快速日志中最后一段包含非提交请求的日志
	Start uint32
	Reqs  []*pb.Request
}

type ResultedCommand struct {
	*pb.Request
	reply     *pb.RequestReply
	oldVal    string
	committed bool
}

var fbfCmdDone = request.RequestID{ClientID: 67890, SeqID: 12345}
var batchedSlowCmdsDone = request.RequestID{ClientID: 12345, SeqID: 67890}

func (t *ResultedCommand) IsCommitted() bool {
	return t.committed
}

// Server-side communication proxy
type Participants_2PC struct { // 这个结构负责client向peers的2PC过程
	trans map[string]Trans // coordinator 与 clients的通信
	// cmdsCache map[request.RequestID]*ResultedCommand // 当前正在处理的xproto.Transaction
	cmdsCache *cmdsCache

	// keyCache_acc map[string]*Sepculate_Result_tx // 当前占据某个key的交易

	// currentSlowKey map[string]struct{} // 当前处于slowpath的key

	fastLogs *fastLogs

	// blocked bool // 当前拒绝fast path and slow path的key
	// 提交的xproto.Transaction
	kvStore xkv.KVStore

	OrderSepculateC chan *MergeInfo
	SlowPathC       chan *pb.Request

	sm       *stateManager
	logger   *zap.SugaredLogger
	isLeader atomic.Uint32

	// mu sync.Mutex

	reqboard  *request.RequestBoard
	mergedeep int

	batchCmds *batchedFCmds

	openBatch bool

	grpcAddrs   []string
	grpcServers []*coordinatorGRPC
}

func NewParticipants_2PC(id int, kv xkv.KVStore, mergeDeep int, grpcAddrs []string) (*Participants_2PC, chan *MergeInfo, chan *pb.Request) {
	p := &Participants_2PC{
		trans:     make(map[string]Trans),
		cmdsCache: newCmdsCache(),
		// keyCache_acc: make(map[string]*Sepculate_Result_tx),
		// currentSlowKey:       make(map[string]struct{}),
		fastLogs:        newFastLogs(),
		kvStore:         kv,
		OrderSepculateC: make(chan *MergeInfo, 1000),
		// slowPathApplyC:       make(chan *slowcmd_commit_helper),
		logger:      xlog.InitLogger().Named(fmt.Sprintf("2PC-%v:", id)),
		isLeader:    atomic.Uint32{},
		reqboard:    request.NewBoard(),
		sm:          newStateManager(),
		mergedeep:   mergeDeep,
		SlowPathC:   make(chan *pb.Request, 1000),
		batchCmds:   &batchedFCmds{mu: sync.Mutex{}, batchRequest: &pb.FBFCmd{}},
		grpcAddrs:   grpcAddrs,
		grpcServers: make([]*coordinatorGRPC, len(grpcAddrs)),
		openBatch:   false,
	}
	p.isLeader.Store(0)
	if len(grpcAddrs) != 0 {
		p.logger.Info("Open batch for blocked cmds.")
		p.openBatch = true
	}
	// p.km = newKeyManager(p.PrepareKeyToFastC)
	return p, p.OrderSepculateC, p.SlowPathC
}

func (p *Participants_2PC) executeClientCommand(cmd *pb.Command, rr *pb.RequestReply) string {
	var res xkv.KvOpStatus
	var err error
	var oldValue string
	var opValue string
	if cmd.Op == pb.PUT {
		oldValue, err = p.kvStore.PutWithOldValue(cmd.Key, cmd.Val)
	} else if cmd.Op == pb.GET {
		opValue, err = p.kvStore.Get(cmd.Key) // Get is no need to rollback
		rr.Val = opValue
	} else if cmd.Op == pb.DELETE {
		oldValue, err = p.kvStore.DeleteWithOldValue(cmd.Key)
	}
	if err != nil {
		res = xkv.KEY_NOT_EXIST
	} else {
		res = xkv.SUCCEED
	}
	rr.OpReply = uint32(res)
	return oldValue
}

func (p *Participants_2PC) Close() { // 2pc 服务
	p.logger.Debugf("closed")
}

func (p *Participants_2PC) merge() {
	// p.currentSlowKey[key] = struct{}{}
	p.logger.Info("set the state to slow")
	start, txs := p.fastLogs.getUncommittedTail(p.mergedeep)
	l := make([]*pb.Request, len(txs))
	for i, v := range txs {
		l[i] = v.Request
	}
	if len(l) == 0 {
		p.logger.Fatalf("the Merge get a nil uncommittedTxs from start %v", start)
	}
	p.OrderSepculateC <- &MergeInfo{Reqs: l, Start: uint32(start)}
}

func (p *Participants_2PC) FollowerMerge(start int, cmds []*pb.Request) {
	requestIDs := make([]request.RequestID, len(cmds))
	for i := range cmds {
		requestIDs[i] = request.GetRequestID(cmds[i])
	}
	p.cmdsCache.batchDelete(requestIDs)
	localStart, speculativeCmds := p.fastLogs.getUncommittedTxs()
	p.logger.Warnf("process a merge info: start from %v: %v, local start %v: %v", start, cmds, localStart, speculativeCmds)
	// 找到第一个日志不一样的位置，然后回滚

	// 先对齐需要同步的日志
	if localStart < start {
		if localStart+len(speculativeCmds) < start {
			// 此时快速日志比leader短，需要recover
			p.logger.DPanic("need to recover")
		} else {
			speculativeCmds = speculativeCmds[start-localStart:]
			localStart = start
		}
	} else {
		start = localStart
		cmds = cmds[start-localStart:]
		minLen := len(cmds)
		if minLen > len(speculativeCmds) {
			minLen = len(speculativeCmds)
		}
		speculativeCmds = speculativeCmds[:minLen]
	}
	k := -1
	for i := range speculativeCmds { // 找到第一个不相等的日志请求，从这个位置开始修复
		if i > len(cmds) {
			k = i
			break
		}
		if request.GetRequestID(cmds[i]) != request.GetRequestID(speculativeCmds[i].Request) {
			p.logger.Warnf("the log is inconsistent at positions %v, expert %v, get %v", i, speculativeCmds[i].Request.String(), cmds[i].String())
			k = i
			break
		}
	}
	if k == -1 && len(cmds) > len(speculativeCmds) {
		k = len(speculativeCmds)
	}
	if k != -1 {
		for i := k; i < len(speculativeCmds); i++ {
			if speculativeCmds[i].Command.Op != pb.GET { // 找到第一个非读的请求，将其回滚
				if speculativeCmds[i].Command.Op == pb.PUT {
					p.kvStore.RollbackPutWithOldValue(speculativeCmds[i].Request.Command.Key, speculativeCmds[i].oldVal)
					break
				} else if speculativeCmds[i].Command.Op == pb.DELETE {
					p.kvStore.RollbackDeleteWithOldValue(speculativeCmds[i].Request.Command.Key, speculativeCmds[i].oldVal)
					break
				}
			} else {

			}
		}
		for i := k; i < len(cmds); i++ {
			p.executeClientCommand(cmds[i].Command, &pb.RequestReply{})
		}
	}
	mergedTxs := make([]*ResultedCommand, len(cmds))
	for i := range cmds {
		// if v, ok :=  p.cmdsCache[request.GetRequestID(cmds[i])]; ok {
		if v, ok := p.cmdsCache.get(request.GetRequestID(cmds[i])); ok {
			mergedTxs[i] = v
		} else {
			mergedTxs[i] = &ResultedCommand{Request: cmds[i], committed: true}
		}
	}
	p.fastLogs.fixUncommittedTxs(localStart, mergedTxs)
	p.fastLogs.clear()
}

func (p *Participants_2PC) LeaderMerge(m *MergeInfo) {
	for i := range m.Reqs {
		cmdID := request.GetRequestID(m.Reqs[i])
		if res, ok := p.cmdsCache.get(cmdID); ok {
			res.reply.ReqReply = uint32(pb.SLOW_SUCCEED) // 这里由于leader 不需要回滚，因此使用旧的结果进行回复
			go func() {
				p.reqboard.InsertMr(cmdID, res.reply)
			}()
			p.commit(cmdID)
		}
	}
	p.fastLogs.clear()
}

func (p *Participants_2PC) WrapReplyWithConflict(rr *pb.RequestReply, conflictReqs []*ResultedCommand) *pb.RequestReply {
	rr.ConflictReqs = make([]*pb.RequestID, len(conflictReqs))
	for i := range conflictReqs {
		rr.ConflictReqs[i] = &pb.RequestID{SeqId: conflictReqs[i].SeqId, ClientID: conflictReqs[i].ClientID}
	}
	return rr
}

func (p *Participants_2PC) Prepare(req *pb.Request) *pb.RequestReply {
	var rr *pb.RequestReply
	rr = &pb.RequestReply{}
	cmdID := request.GetRequestID(req)
	res, ok := p.cmdsCache.get(cmdID)
	if !ok {
		oldVal := p.executeClientCommand(req.Command, rr)
		resCmd := &ResultedCommand{Request: req, oldVal: oldVal, reply: rr} // 暂存这个交易
		p.cmdsCache.insert(cmdID, resCmd)
		if conflictReqs := p.conflictReqs(resCmd); len(conflictReqs) != 0 { // 如果与当前命令冲突
			// p.logger.Debugf("conflict with %v", conflictReqs)
			rr.ReqReply = uint32(pb.PREPARE_CONFLICT)
			rr = p.WrapReplyWithConflict(rr, conflictReqs)
			// 通过慢速路径发送这个key
		} else { // 没有检测到冲突
			// p.logger.Debugf("process command %v without conflict", req.String())
			rr.ReqReply = uint32(pb.PREPARE_SUCCEED)
		}
		p.fastLogs.append(resCmd)
		rr.Fterm = p.sm.getCurrentTerm()
	} else {
		rr = res.reply
		if rr.Fterm < p.sm.getCurrentTerm() {
			// delete(p.cmdsCache, cmdID)
			p.cmdsCache.delete(cmdID)
			return p.Prepare(req)
		}
		// p.logger.Warnf("get a repeat command %v, return the old stat %v", req.String(), rr)
	}
	return rr
}

func (p *Participants_2PC) commit(cmdID request.RequestID) {
	p.cmdsCache.commit(cmdID)
}

func (p *Participants_2PC) AbortReq(cmdID request.RequestID, FTerm uint32) (reply *pb.RequestReply) {
	// p.mu.Lock()

	if _, ok := p.cmdsCache.get(cmdID); ok {
		b, err := p.sm.setMerge(FTerm)
		if err != nil {
			p.logger.Errorf("Merge error: %v", err)
		}
		if b {
			p.merge()
		}
	}

	// p.mu.Unlock()
	ret := p.reqboard.WaitForMr(cmdID)
	rr := ret.(*pb.RequestReply)
	return rr
}

func (p *Participants_2PC) Propose(mess *pb.Message) *pb.MessageReply {
	waitSync := false
	var rr *pb.RequestReply
	if p.sm.fastLiveLock() {
		// for _, seqID := range mess.CommitCmd {
		seqID := mess.CommitCmd
		cmdID := request.RequestID{ClientID: mess.ClientID, SeqID: seqID}
		p.commit(cmdID)
		// }
		if mess.Request != nil {
			rr = p.Prepare(mess.Request)
		}
	} else {
		if p.sm.isBlocked() { // 如果当前key被block了， 说明当前key正在被放入快速路径上
			// leader batch the slowpath command and take a role as coordinator to send the batched cmds as the first fast cmd。
			// p.logger.Debugf("blocked slowpath command %v", mess.Request.SeqId, mess.Request.Command.Key)
			rr = &pb.RequestReply{}
			if p.openBatch && p.batchCmds.batch(mess.Request) {
				p.logger.Debug("batch a blocked request.")
				waitSync = true
				rr.ReqReply = uint32(pb.BACATCH_WHEN_BLOCK)
			} else {
				rr.ReqReply = uint32(pb.CURRENT_PREAPRE_FAST)
			}
		} else {
			p.logger.Debugf("proccessing a slow request %v", mess.Request.String())

			p.SlowPathC <- mess.Request // 将这个tx发送到slowpath上
			waitSync = true
			rr = &pb.RequestReply{}
			rr.ReqReply = uint32(pb.CURRENT_SLOWMODE)
		}
	}
	p.sm.fastLiveUnlock()

	if waitSync && p.isLeader.Load() != 0 { // 只有leader需要等待
		p.logger.Info("wait to sync")
		r := p.reqboard.WaitForMr(request.GetRequestID(mess.Request))
		rr = r.(*pb.RequestReply)
	}

	return &pb.MessageReply{MSeqId: mess.MSeqId, RR: rr, Leader: p.isLeader.Load()}
}

func (p *Participants_2PC) BecomeLeader() {
	p.isLeader.Store(1)
	p.logger.Debug("become a leader")
	if p.openBatch {
		for i, addr := range p.grpcAddrs {
			p.logger.Infof("connect to server %v", addr)
			p.grpcServers[i] = newCoordinatorGRPC(addr) // 开启grpcserver
		}
		p.logger.Debug("batch open.")
	}
}

func (p *Participants_2PC) BecomeNone() {
	p.isLeader.Store(0)
	p.logger.Debug("become a follower")
}

func (p *Participants_2PC) SetToSlow(m *MergeInfo) {
	if p.isLeader.Load() == 0 {
		p.sm.setMerge(p.sm.fterm) // after setMerge
		p.FollowerMerge(int(m.Start), m.Reqs)
	} else {
		p.logger.Info("Leader merge")
		p.LeaderMerge(m)
	}
	p.sm.setSlow()
}

func (p *Participants_2PC) StartBatchCmds() {
	p.batchCmds.startBatch(p.sm.getCurrentTerm()) // the Fterm will increase after the SetFast()
}

func (p *Participants_2PC) ProcessBatchedCmds() {
	notifyC := make(chan struct{}, 10)
	cmd := p.batchCmds.down()
	go func() {
		for _, server := range p.grpcServers {
			go func(s *coordinatorGRPC) {
				s.StartFast(cmd)
				notifyC <- struct{}{}
			}(server)
		}
	}()

	for range p.grpcServers {
		<-notifyC
	}
	p.logger.Debug("Leader process fast batch done.")
}

func (p *Participants_2PC) SetBlockedToFast() {
	p.logger.Info("blocked to prepare Fast")
	if p.openBatch {
		p.logger.Debug("start batch the blocked cmds.")
		p.StartBatchCmds()
	}
	p.sm.setBlocked()
}

func (p *Participants_2PC) SetFast(term uint32) {
	p.logger.Info("change fast")
	s, err := p.sm.canSetFast(term)
	if err != nil {
		p.logger.Fatal(err)
	}
	if !s {
		p.logger.Warnf("change to fast term %v failed.", term)
	} else {
		if p.openBatch {
			p.logger.Infof("wait for the FBFCmds to be executed.")
			if p.isLeader.Load() != 0 {
				p.ProcessBatchedCmds() // leader process the batched cmds before start fast
			}
			p.reqboard.WaitForMr(fbfCmdDone) // wait for the FBFCmds be executed before start fast
			p.reqboard.ClearID(fbfCmdDone)
		}
		p.logger.Infof("change to Fast with term %v", term)
		p.sm.setFastAndUnblocked(term)
	}

}

func (p *Participants_2PC) ExecuteSlowCommand(slowCmd *pb.Request) {
	r := &pb.RequestReply{ReqReply: uint32(pb.SLOW_SUCCEED)}
	// p.mu.Lock()
	p.executeClientCommand(slowCmd.Command, r)
	// p.mu.Unlock()
	p.reqboard.InsertMr(request.GetRequestID(slowCmd), r)
}

// execute the last batch slow cmd and ready to answer the leaders first batched fast cmds(FBFCmds)
func (p *Participants_2PC) ExecuteBatchedSlowCommand(cmds []*pb.Request) {
	p.logger.Infof("start execute BatchedSlowCommand")
	for _, cmd := range cmds {
		p.ExecuteSlowCommand(cmd)
	}
	// notify that the slow cmd are all finished
	if p.openBatch {
		p.reqboard.InsertMr(batchedSlowCmdsDone, struct{}{})
	}
}

func (p *Participants_2PC) ExecuteFBFCmds(bcmds *pb.FBFCmd) {
	// todo: check the state
	p.logger.Infof("start execute FBFCmds")
	p.reqboard.WaitForMr(batchedSlowCmdsDone) // execute FBFcmds after the last batched slow cmds
	p.reqboard.ClearID(batchedSlowCmdsDone)
	p.logger.Infof("wait batched slow commands done")
	for _, cmd := range bcmds.Cmds {
		r := &pb.RequestReply{ReqReply: uint32(pb.BATCHED_FAST_SUCCEED)}
		p.executeClientCommand(cmd.Command, r)
		if p.isLeader.Load() != 0 {
			p.reqboard.InsertMr(request.GetRequestID(cmd), r) // leader通知由于block而等待的cmd
		}
	}
	// TODO: Append the FBFCmds to the Fast log for fault tolerance
	// because FBFCmds is the first request processed in fast mode, it is no need to consider conflicts with it.
	// notify that the FBFCmds are executed, so can change to fast mode
	p.reqboard.InsertMr(fbfCmdDone, struct{}{})

	p.logger.Infof("insert fast batch commands done marker")
}

// 返回与tx相冲突的command
func (p *Participants_2PC) conflictReqs(resCmd *ResultedCommand) []*ResultedCommand {
	conflictReqs := make([]*ResultedCommand, 0)
	_, reqs := p.fastLogs.getUncommittedTxs()
	for i := range reqs {
		if reqs[i].committed {
			continue
		}

		if pb.Conflict(reqs[i].Request.Command, resCmd.Request.Command) {
			conflictReqs = append(conflictReqs, reqs[i])
		}
	}
	// p.cmdsCache[cmdID] = recmd

	return conflictReqs
}

func (p *Participants_2PC) Ping() int64 {
	return 0
}
