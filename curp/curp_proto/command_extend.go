package curp_proto

import (
	"bytes"
	"encoding/gob"
	"github/Fischer0522/xraft/curp/command"
	"log"
)

func (c *CurpClientCommand) Encode() string {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	encoder.Encode(c)
	return buf.String()
}

func DecodeClientCommand(content string) CurpClientCommand {
	var c CurpClientCommand
	decoder := gob.NewDecoder(bytes.NewBufferString(content))
	if err := decoder.Decode(&c); err != nil {
		log.Fatalf("could not decode message (%v)", err)
	}
	return c
}

func (c *CurpClientCommand) ProposeId() command.ProposeId {
	return command.ProposeId{
		SeqId:    c.SeqId,
		ClientId: c.ClientId,
	}
}

type CurpStatusCode uint8

const (
	ACCEPTED uint32 = iota
	CONFLICT
	TIMEOUT
)

func Conflict(c1, c2 *CurpClientCommand) bool {
	if c1.Key == c2.Key {
		if c1.Op == command.PUT || c2.Op == command.PUT {
			return true
		}

		if c1.Op == command.DELETE || c2.Op == command.DELETE {
			return true
		}
	}
	return false
}

func ConflictBatch(batch1 []*CurpClientCommand, batch2 []*CurpClientCommand) bool {
	for i := 0; i < len(batch1); i++ {
		for j := 0; j < len(batch2); j++ {
			if Conflict(batch1[i], batch2[j]) {
				return true
			}
		}
	}
	return false
}

func IsRead(cmd *CurpClientCommand) bool {
	return cmd.Op == command.GET
}
