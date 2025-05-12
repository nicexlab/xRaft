package pb

// type Operation uint32

const (
	PUT uint32 = iota
	GET
	DELETE
	RLOCK
	WLOCK
)

type CommandType uint16

const (
	Cmd CommandType = iota
	ClientCmd

	SlowCmd

	BatchedSCmd

	SetSlowCmd
	PrepareFastCmd
	CollectCmd
	OrderCmd
)

// Notice: we use BigEndian to encode and decode command
// type Command interface {
// 	Encode() []byte
// }

// type ClientCommand struct {
// 	Op    Operation // xraft use this field to mark the FAST-Term in prepare
// 	Key   string    //
// 	Value string
// }

func Conflict(c1, c2 *Command) bool {
	if c1.Key == c2.Key {
		if c1.Op == PUT || c2.Op == PUT {
			return true
		}

		if c1.Op == DELETE || c2.Op == DELETE {
			return true
		}
	}
	return false
}

func (c *Command) Equal(c2 *Command) bool {
	return c.Op == c2.Op && c.Key == c2.Key && c.Val == c2.Val
}

// func (c *ClientCommand) EncodeCommand() []byte {
// 	// use uint16 to encode the op

// 	buf := make([]byte, 0)
// 	// use uint16 to encode the op
// 	buf = binary.BigEndian.AppendUint16(buf, uint16(ClientCmd))
// 	buf = binary.BigEndian.AppendUint32(buf, uint32(c.Op))
// 	buf = binary.BigEndian.AppendUint16(buf, uint16(len(c.Key)))
// 	buf = binary.BigEndian.AppendUint16(buf, uint16(len(c.Val)))
// 	buf = append(buf, []byte(c.Key)...)
// 	buf = append(buf, []byte(c.Val)...)
// 	return buf
// }

// func DecodeClientCommand(buf []byte) *ClientCommand {
// 	c := &ClientCommand{}
// 	// pb.Unmarshal(buf, c)
// 	op := binary.BigEndian.Uint32(buf[0:4])
// 	c.Op = op
// 	buf = buf[4:]
// 	keyLen := binary.BigEndian.Uint16(buf[0:2])
// 	buf = buf[2:]
// 	valueLen := binary.BigEndian.Uint16(buf[0:2])
// 	buf = buf[2:]
// 	c.Key = string(buf[0:keyLen])
// 	buf = buf[keyLen:]
// 	c.Val = string(buf[0:valueLen])

// 	return c
// }

// func Conflict(c1, c2 *ClientCommand) bool {
// 	if c1.Key == c2.Key {
// 		if c1.Op == PUT || c2.Op == PUT {
// 			return true
// 		}
// 	}
// 	return false
// }

// func ConflictBatch(batch1 []ClientCommand, batch2 []ClientCommand) bool {
// 	for i := 0; i < len(batch1); i++ {
// 		for j := 0; j < len(batch2); j++ {
// 			if Conflict(&batch1[i], &batch2[j]) {
// 				return true
// 			}
// 		}
// 	}
// 	return false
// }

// func IsRead(command *ClientCommand) bool {
// 	return command.Op == GET
// }
