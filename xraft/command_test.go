package xraft

import (
	"encoding/binary"

	"github/Fischer0522/xraft/xraft/conn"
	"github/Fischer0522/xraft/xraft/pb"
	"math/rand"
	"testing"
	"time"
)

func ValidateCommandType(t *testing.T, buf []byte, expected pb.CommandType) []byte {
	actual := pb.CommandType(binary.BigEndian.Uint16(buf[0:2]))
	if actual != expected {
		t.Errorf("DecodeCommandType() = %v, want %v", actual, expected)
	}
	buf = buf[2:]
	return buf
}

// func TestDecodeClientCommand(t *testing.T) {
// 	cmd := pb.ClientCommand{
// 		Value: "test",
// 		Op:    xproto.PUT,
// 		Key:   "key",
// 	}
// 	buf := cmd.EncodeCommand()
// 	buf = ValidateCommandType(t, buf, xproto.ClientCmd)
// 	decodedCmd := xproto.DecodeClientCommand(buf)
// 	if !cmd.Equal(decodedCmd) {
// 		t.Errorf("DecodeClientCommand() = %v, want %v", decodedCmd.String(), cmd.String())
// 	}
// }

func TestDecodeSetSlowCommand(t *testing.T) {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	cmd := &conn.MergeInfo{Start: 31, Reqs: make([]*pb.Request, 100)}
	for i := range cmd.Reqs {
		cmd.Reqs[i] = &pb.Request{SeqId: rand.Uint64(), ClientID: rand.Uint64(), ReqOp: rand.Uint32()}
		cmd.Reqs[i].Command = &pb.Command{Op: rand.Uint32(), Key: RandomString(5, rand), Val: RandomString(5, rand)}
	}

	buf := EncodeMergInfo(cmd)
	buf = ValidateCommandType(t, buf, pb.SetSlowCmd)
	decodedCmd := DecodeSetSlowCommand(buf)
	if len(cmd.Reqs) != len(decodedCmd.Reqs) {
		t.Errorf("DecodePrepareCommand() = decode key lens %v, want %v", len(decodedCmd.Reqs), len(cmd.Reqs))
	}
	if cmd.Start != decodedCmd.Start {
		t.Errorf("DecodePrepareCommand() = decodedCmd.Start %v, want %v", decodedCmd.Start, cmd.Start)
	}
	for i := range cmd.Reqs {
		if cmd.Reqs[i].String() != decodedCmd.Reqs[i].String() {
			t.Errorf("DecodePrepareCommand() = decodedCmd.Reqs %v, want %v", decodedCmd.Reqs[i].String(), cmd.Reqs[i].String())
		}
	}
}

func TestDecodePrepareFastCommand(t *testing.T) {

	cmd := PrepareFastCommand{}
	buf := cmd.Encode()
	buf = ValidateCommandType(t, buf, pb.PrepareFastCmd)
	if len(buf) != 0 {
		t.Errorf("DecodePrepareCommand() = want nil buf")
	}
}

// func TestDecodeOrderCommand(t *testing.T) {
// 	cmd := core.OrderCommand{
// 		CommandIds: make([]core.CommandId, 0),
// 		Key:        "x1v3",
// 		FastTerm:   1,
// 	}
// 	cmd.CommandIds = append(cmd.CommandIds, core.CommandId{Client: "abc", Nonce: 123}, core.CommandId{Client: "efg", Nonce: 456}, core.CommandId{Client: "hijgg", Nonce: 789})
// 	buf := cmd.Encode()
// 	buf = ValidateCommandType(t, buf, xproto.OrderCmd)
// 	decodedCmd := core.DecodeOrderCommand(buf)
// 	for i := 0; i < len(cmd.CommandIds); i++ {
// 		if cmd.CommandIds[i] != decodedCmd.CommandIds[i] {
// 			t.Errorf("DecodeOrderCommand() = %v, want %v", decodedCmd, cmd)
// 		}
// 	}
// 	if cmd.Key != decodedCmd.Key {
// 		t.Errorf("DecodeOrderCommand() = %v, want %v", decodedCmd, cmd)
// 	}
// 	if cmd.FastTerm != decodedCmd.FastTerm {
// 		t.Errorf("DecodeOrderCommand() = %v, want %v", decodedCmd, cmd)
// 	}
// }

// func TestDecodeOrderLargeScale(t *testing.T) {
// 	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
// 	cmdIds := 1000
// 	cmd := core.OrderCommand{
// 		CommandIds: make([]core.CommandId, 0),
// 		Key:        "x1v3",
// 		FastTerm:   1,
// 	}
// 	for i := 0; i < cmdIds; i++ {
// 		cmd.CommandIds = append(cmd.CommandIds, core.CommandId{Client: RandomString(5, rand)})
// 	}
// 	buf := cmd.Encode()
// 	buf = ValidateCommandType(t, buf, xproto.OrderCmd)
// 	decodedCmd := core.DecodeOrderCommand(buf)
// 	if len(cmd.CommandIds) != len(decodedCmd.CommandIds) {
// 		t.Errorf("DecodeOrderCommand() = %v, want %v", decodedCmd, cmd)
// 	}
// 	for i := 0; i < len(cmd.CommandIds); i++ {
// 		if cmd.CommandIds[i] != decodedCmd.CommandIds[i] {
// 			t.Errorf("DecodeOrderCommand() = %v, want %v", decodedCmd, cmd)
// 		}
// 	}
// 	if cmd.Key != decodedCmd.Key {
// 		t.Errorf("DecodeOrderCommand() = %v, want %v", decodedCmd, cmd)
// 	}
// 	if cmd.FastTerm != decodedCmd.FastTerm {
// 		t.Errorf("DecodeOrderCommand() = %v, want %v", decodedCmd, cmd)
// 	}

// }

// func TestDecodeCollectCommand(t *testing.T) {
// 	cmd := core.CollectCommand{
// 		FastTerm: 1,
// 	}
// 	buf := cmd.Encode()
// 	buf = ValidateCommandType(t, buf, xproto.CollectCmd)
// 	decodedCmd := core.DecodeCollectCommand(buf)
// 	if cmd != decodedCmd {
// 		t.Errorf("DecodeCollectCommand() = %v, want %v", decodedCmd, cmd)
// 	}
// }

func TestDecodeSlowCommand(t *testing.T) {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	slowcmd := &pb.Request{SeqId: rand.Uint64(), ClientID: rand.Uint64(), ReqOp: rand.Uint32()}
	slowcmd.Command = &pb.Command{Op: rand.Uint32(), Key: RandomString(5, rand), Val: RandomString(5, rand)}
	buf := EncodeRequestWithSlow(slowcmd)
	buf = ValidateCommandType(t, buf, pb.SlowCmd)
	decodedCmd, _ := DecodeRequest(buf)
	if slowcmd.String() != decodedCmd.String() {
		t.Errorf("DecodeCollectCommand() = %v, want %v", decodedCmd.String(), slowcmd.String())
	}
}

func TestEncodeBatchedCmds(t *testing.T) {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	cmd := make([]*pb.Request, 10)

	for i := range cmd {
		cmd[i] = &pb.Request{SeqId: rand.Uint64(), ClientID: rand.Uint64(), ReqOp: rand.Uint32()}
	}

	buf := EncodeBatchedSlowCmds(cmd)
	buf = ValidateCommandType(t, buf, pb.BatchedSCmd)
	decodedCmd := DecodeBatchedSlowCmds(buf)
	if len(cmd) != len(decodedCmd) {
		t.Errorf("DecodePrepareCommand() = decode key lens %v, want %v", len(decodedCmd), len(cmd))
	}
	for i := range cmd {
		if cmd[i].String() != decodedCmd[i].String() {
			t.Errorf("DecodePrepareCommand() = decodedCmd.Reqs %v, want %v", decodedCmd[i].String(), cmd[i].String())
		}
	}
}
