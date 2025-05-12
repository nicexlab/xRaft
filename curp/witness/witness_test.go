package witness

import (
	"github/Fischer0522/xraft/curp/command"
	"github/Fischer0522/xraft/curp/curp_proto"
	"testing"
)

func TestSimpleConflict(t *testing.T) {
	c1 := &curp_proto.CurpClientCommand{
		Key: "key",
		Op:  command.PUT,
	}
	c2 := &curp_proto.CurpClientCommand{
		Key: "key",
		Op:  command.PUT,
	}
	witness := NewWitness()
	witness.InsertIfNotConflict(c1)
	if !witness.InsertIfNotConflict(c2) {
		t.Errorf("Conflict should be detected")
	}
}

func TestAllRead(t *testing.T) {
	c1 := &curp_proto.CurpClientCommand{
		Key: "key",
		Op:  command.GET,
	}
	c2 := &curp_proto.CurpClientCommand{
		Key: "key",
		Op:  command.GET,
	}
	witness := NewWitness()
	witness.InsertIfNotConflict(c1)
	if witness.InsertIfNotConflict(c2) {
		t.Errorf("Conflict should not be detected")
	}
}

func TestDifferentKey(t *testing.T) {
	c1 := &curp_proto.CurpClientCommand{
		Key: "key1",
		Op:  command.PUT,
	}
	c2 := &curp_proto.CurpClientCommand{
		Key: "key2",
		Op:  command.PUT,
	}
	witness := NewWitness()
	witness.InsertIfNotConflict(c1)
	if witness.InsertIfNotConflict(c2) {
		t.Errorf("Conflict should not be detected")
	}
}

func TestRemove(t *testing.T) {
	c1 := &curp_proto.CurpClientCommand{
		Key:   "key",
		Op:    command.GET,
		SeqId: 1,
	}
	c2 := &curp_proto.CurpClientCommand{
		Key:   "key",
		Op:    command.PUT,
		SeqId: 2,
	}
	c3 := &curp_proto.CurpClientCommand{
		Key:   "key",
		Op:    command.GET,
		SeqId: 3,
	}
	witness := NewWitness()
	witness.InsertIfNotConflict(c1)
	witness.InsertIfNotConflict(c3)
	if !witness.InsertIfNotConflict(c2) {
		t.Errorf("Conflict should be detected")
	}

	witness.Remove(c1)

	if !witness.InsertIfNotConflict(c2) {
		t.Errorf("Conflict should be detected")
	}
	witness.Remove(c3)
	if witness.InsertIfNotConflict(c2) {
		t.Errorf("Conflict should not be detected")
	}
}
