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

var FBFCmdDone request.RequestID = request.RequestID{ClientID: 67890, SeqID: 12345}
var BatchedSlowcmdsDone request.RequestID = request.RequestID{ClientID: 12345, SeqID: 67890}

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

	open_batch bool

	grpcAddrs   []string
	grpcServers []*coor_grpc
}

func NewParticipants_2PC(id int, kv xkv.KVStore, mergeDeep int, grpcAddrs []string) (*Participants_2PC, chan *MergeInfo, chan *pb.Request) {
	p := &Participants_2PC{
		trans:     make(map[string]Trans),
		cmdsCache: newCmdsCache(),
		// keyCache_acc: make(map[string]*Sepculate_Result_tx),
		// currentSlowKey:       make(map[string]struct{}),
		fastLogs:        NewFastLogs(),
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
		grpcServers: make([]*coor_grpc, len(grpcAddrs)),
		open_batch:  false,
	}
	p.isLeader.Store(0)
	if len(grpcAddrs) != 0 {
		p.logger.Info("Open batch for blocked cmds.")
		p.open_batch = true
	}
	// p.km = newKeyManager(p.PrepareKeyToFastC)
	return p, p.OrderSepculateC, p.SlowPathC
}

func (p *Participants_2PC) executeClientCommand(cmd *pb.Command, rr *pb.RequestReply) string {
	var res xkv.KvOpStatus
	var err error
	// var val string
	var old_value string
	var op_value string
	if cmd.Op == pb.PUT {
		old_value, err = p.kvStore.PutWithOldValue(cmd.Key, cmd.Val)
	} else if cmd.Op == pb.GET {
		op_value, err = p.kvStore.Get(cmd.Key) // Get is no need to rollback
		rr.Val = op_value
	} else if cmd.Op == pb.DELETE {
		old_value, err = p.kvStore.DeleteWithOldValue(cmd.Key)
	}
	if err != nil {
		res = xkv.KEY_NOT_EXIST
	} else {
		res = xkv.SUCCEED
	}
	rr.OpReply = uint32(res)
	return old_value
}

func (p *Participants_2PC) Close() { // 2pc 服务
	p.logger.Debugf("closed")
}

func (p *Participants_2PC) merge() {
	// p.currentSlowKey[key] = struct{}{}
	p.logger.Info("set the state to slow")
	start, txs := p.fastLogs.GetUncommittedTail(p.mergedeep)
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
	requestIds := make([]request.RequestID, len(cmds))
	for i := range cmds {
		requestIds[i] = request.GetRequestID(cmds[i])
	}
	p.cmdsCache.batchDelete(requestIds)
	local_start, sepculate_cmds := p.fastLogs.GetUncommittedTxs()
	p.logger.Warnf("process a merge info: start from %v: %v, local start %v: %v", start, cmds, local_start, sepculate_cmds)
	// 找到第一个日志不一样的位置，然后回滚

	// 先对齐需要同步的日志
	if local_start < start {
		if local_start+len(sepculate_cmds) < start {
			// 此时快速日志比leader短，需要recover
			p.logger.DPanic("need to recover")
		} else {
			sepculate_cmds = sepculate_cmds[start-local_start:]
			local_start = start
		}
	} else {
		start = local_start
		cmds = cmds[start-local_start:]
		min_len := len(cmds)
		if min_len > len(sepculate_cmds) {
			min_len = len(sepculate_cmds)
		}
		sepculate_cmds = sepculate_cmds[:min_len]
	}
	k := -1
	for i := range sepculate_cmds { // 找到第一个不相等的日志请求，从这个位置开始修复
		if i > len(cmds) {
			k = i
			break
		}
		if request.GetRequestID(cmds[i]) != request.GetRequestID(sepculate_cmds[i].Request) {
			p.logger.Warnf("the log is inconsistent at positions %v, expert %v, get %v", i, sepculate_cmds[i].Request.String(), cmds[i].String())
			k = i
			break
		}
	}
	if k == -1 && len(cmds) > len(sepculate_cmds) {
		k = len(sepculate_cmds)
	}
	if k != -1 {
		for i := k; i < len(sepculate_cmds); i++ {
			if sepculate_cmds[i].Command.Op != pb.GET { // 找到第一个非读的请求，将其回滚
				if sepculate_cmds[i].Command.Op == pb.PUT {
					p.kvStore.RollbackPutWithOldValue(sepculate_cmds[i].Request.Command.Key, sepculate_cmds[i].oldVal)
					break
				} else if sepculate_cmds[i].Command.Op == pb.DELETE {
					p.kvStore.RollbackDeleteWithOldValue(sepculate_cmds[i].Request.Command.Key, sepculate_cmds[i].oldVal)
					break
				}
			} else {

			}
		}
		for i := k; i < len(cmds); i++ {
			p.executeClientCommand(cmds[i].Command, &pb.RequestReply{})
		}
	}
	merged_txs := make([]*ResultedCommand, len(cmds))
	for i := range cmds {
		// if v, ok :=  p.cmdsCache[request.GetRequestID(cmds[i])]; ok {
		if v, ok := p.cmdsCache.get(request.GetRequestID(cmds[i])); ok {
			merged_txs[i] = v
		} else {
			merged_txs[i] = &ResultedCommand{Request: cmds[i], committed: true}
		}
	}
	p.fastLogs.FixUncommittedTxs(local_start, merged_txs)
	p.fastLogs.PersistenceKey()
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
	p.fastLogs.PersistenceKey()
}

func (p *Participants_2PC) WarpReplyWithConflict(rr *pb.RequestReply, conflict_reqs []*ResultedCommand) *pb.RequestReply {
	rr.ConflictReqs = make([]*pb.RequestID, len(conflict_reqs))
	for i := range conflict_reqs {
		rr.ConflictReqs[i] = &pb.RequestID{SeqId: conflict_reqs[i].SeqId, ClientID: conflict_reqs[i].ClientID}
	}
	return rr
}

func (p *Participants_2PC) Prepare(req *pb.Request) *pb.RequestReply {
	var rr *pb.RequestReply
	rr = &pb.RequestReply{}
	cmdID := request.GetRequestID(req)
	res, ok := p.cmdsCache.get(cmdID)
	if !ok {
		oldval := p.executeClientCommand(req.Command, rr)
		rescmd := &ResultedCommand{Request: req, oldVal: oldval, reply: rr} // 暂存这个交易
		p.cmdsCache.insert(cmdID, rescmd)
		if conflict_reqs := p.conflictReqs(rescmd); len(conflict_reqs) != 0 { // 如果与当前命令冲突
			// p.logger.Debugf("conflict with %v", conflict_reqs)
			rr.ReqReply = uint32(pb.PREPARE_CONFLICT)
			rr = p.WarpReplyWithConflict(rr, conflict_reqs)
			// 通过慢速路径发送这个key
		} else { // 没有检测到冲突
			// p.logger.Debugf("process command %v without conflict", req.String())
			rr.ReqReply = uint32(pb.PREPARE_SUCCEED)
		}
		p.fastLogs.Append(rescmd)
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
	waitsync := false
	var rr *pb.RequestReply
	if p.sm.fastLiveLock() {
		// for _, seqID := range mess.CommitCmd {
		seqID := mess.CommitCmd
		cmdId := request.RequestID{ClientID: mess.ClientID, SeqID: seqID}
		p.commit(cmdId)
		// }
		if mess.Request != nil {
			rr = p.Prepare(mess.Request)
		}
	} else {
		if p.sm.isBlocked() { // 如果当前key被block了， 说明当前key正在被放入快速路径上
			// leader batch the slowpath command and take a role as coordinator to send the batched cmds as the first fast cmd。
			// p.logger.Debugf("blocked slowpath command %v", mess.Request.SeqId, mess.Request.Command.Key)
			rr = &pb.RequestReply{}
			if p.open_batch && p.batchCmds.batch(mess.Request) {
				p.logger.Debug("batch a blocked request.")
				waitsync = true
				rr.ReqReply = uint32(pb.BACATCH_WHEN_BLOCK)
			} else {
				rr.ReqReply = uint32(pb.CURRENT_PREAPRE_FAST)
			}
		} else {
			p.logger.Debugf("proccessing a slow request %v", mess.Request.String())

			p.SlowPathC <- mess.Request // 将这个tx发送到slowpath上
			waitsync = true
			rr = &pb.RequestReply{}
			rr.ReqReply = uint32(pb.CURRENT_SLOWMODE)
		}
	}
	p.sm.fastLiveUnlock()

	if waitsync && p.isLeader.Load() != 0 { // 只有leader需要等待
		p.logger.Info("wait to sync")
		r := p.reqboard.WaitForMr(request.GetRequestID(mess.Request))
		rr = r.(*pb.RequestReply)
	}

	return &pb.MessageReply{MSeqId: mess.MSeqId, RR: rr, Leader: p.isLeader.Load()}
}

func (p *Participants_2PC) BecomeLeader() {
	p.isLeader.Store(1)
	p.logger.Debug("become a leader")
	if p.open_batch {
		for i, addr := range p.grpcAddrs {
			p.logger.Infof("connect to server %v", addr)
			p.grpcServers[i] = new_coor_grpc(addr) // 开启grpcserver
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
	p.batchCmds.startbatch(p.sm.getCurrentTerm()) // the Fterm will increase after the SetFast()
}

func (p *Participants_2PC) ProcessBatchedCmds() {
	notifyC := make(chan struct{}, 10)
	cmd := p.batchCmds.down()
	go func() {
		for _, server := range p.grpcServers {
			go func(s *coor_grpc) {
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
	if p.open_batch {
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
		if p.open_batch {
			p.logger.Infof("wait for the FBFCmds to be executed.")
			if p.isLeader.Load() != 0 {
				p.ProcessBatchedCmds() // leader process the batched cmds before start fast
			}
			p.reqboard.WaitForMr(FBFCmdDone) // wait for the FBFCmds be executed before start fast
			p.reqboard.ClearID(FBFCmdDone)
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
	if p.open_batch {
		p.reqboard.InsertMr(BatchedSlowcmdsDone, struct{}{})
	}
}

func (p *Participants_2PC) ExecuteFBFCmds(bcmds *pb.FBFCmd) {
	// todo: check the state
	p.logger.Infof("start execute FBFCmds")
	p.reqboard.WaitForMr(BatchedSlowcmdsDone) // execute FBFcmds after the last batched slow cmds
	p.reqboard.ClearID(BatchedSlowcmdsDone)
	p.logger.Infof("wait BatchedSlowcmdsDone")
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
	p.reqboard.InsertMr(FBFCmdDone, struct{}{})

	p.logger.Infof("InsertMe FBFCmdDone")
}

// 返回与tx相冲突的command
func (p *Participants_2PC) conflictReqs(rescmd *ResultedCommand) []*ResultedCommand {
	conflict_reqs := make([]*ResultedCommand, 0)
	_, reqs := p.fastLogs.GetUncommittedTxs()
	for i := range reqs {
		if reqs[i].committed {
			continue
		}

		if pb.Conflict(reqs[i].Request.Command, rescmd.Request.Command) {
			conflict_reqs = append(conflict_reqs, reqs[i])
		}
	}
	// p.cmdsCache[cmdID] = recmd

	return conflict_reqs
}

func (p *Participants_2PC) Ping() int64 {
	return 0
}
