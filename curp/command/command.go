package command

import (
	"bytes"
	"encoding/gob"
	"log"
)

type ProposeId struct {
	ClientId uint64
	SeqId    uint64
}

type Operation uint32

const (
	PUT uint32 = iota
	GET
	DELETE
	RLOCK
	WLOCK
)

var OpFmt = []string{"PUT", "GET", "DELETE", "RLOCK", "WLOCK"}

// Notice: we use BigEndian to encode and decode command

type ClientCommand struct {
	ProposeId ProposeId
	Op        uint32 // xraft use this field to mark the FAST-Term in prepare
	Key       string //
	Value     string
}

func (c *ClientCommand) Encode() string {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	encoder.Encode(c)
	return buf.String()
}

func DecodeClientCommand(content string) ClientCommand {
	var c ClientCommand
	decoder := gob.NewDecoder(bytes.NewBufferString(content))
	if err := decoder.Decode(&c); err != nil {
		log.Fatalf("could not decode message (%v)", err)
	}
	return c
}
