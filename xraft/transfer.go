package xraft

import (
	"encoding/binary"
	"github/Fischer0522/xraft/xraft/conn"
	"github/Fischer0522/xraft/xraft/pb"

	"google.golang.org/protobuf/proto"
)

type CollectCommand struct {
	FastTerm int32
}

func (c *CollectCommand) Encode() []byte {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint16(buf, uint16(pb.CollectCmd))
	buf = binary.BigEndian.AppendUint32(buf, uint32(c.FastTerm))
	return buf
}
func DecodeCollectCommand(buf []byte) CollectCommand {
	c := CollectCommand{}
	c.FastTerm = int32(binary.BigEndian.Uint32(buf))
	return c
}

type PrepareFastCommand struct {
	FastTerm uint32
	// Keys []string
}

func (c *PrepareFastCommand) Encode() []byte {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint16(buf, uint16(pb.PrepareFastCmd))
	buf = binary.BigEndian.AppendUint32(buf, uint32(c.FastTerm))
	// buf = binary.BigEndian.AppendUint16(buf, uint16(len(c.Keys)))
	// for i := range c.Keys {
	// 	// key_bs := []byte(c.AcceptTx[i])
	// 	key_bs := []byte(c.Keys[i])
	// 	buf = binary.BigEndian.AppendUint16(buf, uint16(len(key_bs)))
	// 	buf = append(buf, key_bs...)
	// }
	return buf
}

func DecodePrepareFastCommand(buf []byte) PrepareFastCommand {
	c := PrepareFastCommand{}
	c.FastTerm = binary.BigEndian.Uint32(buf[0:4])
	// c.FastTerm = int32(binary.BigEndian.Uint32(buf))
	// c.Keys = make([]string, binary.BigEndian.Uint16(buf[0:2]))
	// buf = buf[2:]
	// for i := range c.Keys {
	// 	key_len := binary.BigEndian.Uint16(buf[0:2])
	// 	c.Keys[i] = string(buf[2 : 2+key_len])
	// 	buf = buf[2+key_len:]
	// }
	return c
}

func EncodeBatchedSlowCmds(r []*pb.Request) []byte {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint16(buf, uint16(pb.BatchedSCmd))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(r)))
	for i := range r {
		buf = append(buf, EncodeRequest(r[i])...)
	}
	return buf
}

func DecodeBatchedSlowCmds(buf []byte) []*pb.Request {
	ll := binary.BigEndian.Uint32(buf[0:4])
	buf = buf[4:]

	ret := make([]*pb.Request, ll)

	for i := range ret {
		ret[i], buf = DecodeRequest(buf)
	}
	return ret
}

// type SetSlowCommand struct {
// 	// FastTerm int32
// 	Starts uint32
// 	Cmds   []*pb.Request
// 	// AcceptTx []*conn.Slow_Command // 当发生冲突时，Leader看到的那个前序的已经Accept的交易
// 	// RejectTx []*conn.Slow_Command
// }

func EncodeRequestWithSlow(r *pb.Request) []byte {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint16(buf, uint16(pb.SlowCmd))
	b, _ := proto.Marshal(r)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(b)))
	buf = append(buf, b...)
	return buf
}

func EncodeRequest(r *pb.Request) []byte {
	buf := make([]byte, 0)
	b, _ := proto.Marshal(r)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(b)))
	buf = append(buf, b...)
	return buf
}

func DecodeRequest(buf []byte) (*pb.Request, []byte) {
	ll := binary.BigEndian.Uint32(buf[0:4])
	buf = buf[4:]
	r := &pb.Request{}
	proto.Unmarshal(buf[:ll], r)
	buf = buf[ll:]
	return r, buf
}

func EncodeMergInfo(m *conn.MergeInfo) []byte {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint16(buf, uint16(pb.SetSlowCmd))
	buf = binary.BigEndian.AppendUint32(buf, m.Start)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(m.Reqs)))
	for i := range m.Reqs {
		buf = append(buf, EncodeRequest(m.Reqs[i])...)
	}
	return buf
}

func DecodeSetSlowCommand(buf []byte) *conn.MergeInfo {
	m := &conn.MergeInfo{}

	m.Start = binary.BigEndian.Uint32(buf[0:4])
	buf = buf[4:]
	l1 := binary.BigEndian.Uint32(buf[0:4])
	buf = buf[4:]

	m.Reqs = make([]*pb.Request, l1)
	for i := range m.Reqs {
		m.Reqs[i], buf = DecodeRequest(buf)
	}
	return m
}

type CommandId struct {
	Client string
	Nonce  uint64
}

type OrderCommand struct {
	FastTerm int32
	// TODO: rename it
	Key        string
	CommandIds []CommandId // 对同一类key修改的命令
}

func (c *OrderCommand) Encode() []byte {
	buf := make([]byte, 0)
	buf = binary.BigEndian.AppendUint16(buf, uint16(pb.OrderCmd))

	buf = binary.BigEndian.AppendUint32(buf, uint32(c.FastTerm))
	key_bs := []byte(c.Key)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(key_bs)))
	buf = append(buf, key_bs...)

	// encode command ids
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(c.CommandIds)))
	for _, v := range c.CommandIds {
		// buf = binary.BigEndian.AppendUint32(buf, uint32(v.client))
		client_bs := []byte(v.Client)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(client_bs)))
		buf = append(buf, client_bs...)
		buf = binary.BigEndian.AppendUint64(buf, uint64(v.Nonce))
	}
	return buf
}

func DecodeOrderCommand(buf []byte) OrderCommand {
	var c OrderCommand
	c.FastTerm = int32(binary.BigEndian.Uint32(buf[0:4]))
	buf = buf[4:]
	key_len := binary.BigEndian.Uint32(buf[0:4])
	c.Key = string(buf[4 : 4+key_len])
	buf = buf[4+key_len:]
	c.CommandIds = make([]CommandId, binary.BigEndian.Uint32(buf[0:4]))
	buf = buf[4:]
	for i := 0; i < len(c.CommandIds); i++ {
		// c.CommandIds[i].client = binary.BigEndian.Uint32(buf[0:4])
		key_len := binary.BigEndian.Uint32(buf[0:4])
		c.CommandIds[i].Client = string(buf[4 : 4+key_len])
		buf = buf[4+key_len:]
		// c.CommandIds[i] = make([]int32, binary.BigEndian.Uint32(buf[0:4]))
		// buf = buf[4:]
		c.CommandIds[i].Nonce = uint64(binary.BigEndian.Uint64(buf[0:8]))
		buf = buf[8:]
	}
	return c
}

func DecodeCommandType(buf []byte) pb.CommandType {
	// decode uint16
	commandType := binary.BigEndian.Uint16(buf)
	return pb.CommandType(commandType)
}
