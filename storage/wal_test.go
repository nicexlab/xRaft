package storage

import (
	"os"
	"testing"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

func clean() {
	os.Remove("test.wal")
}

func buildState(idx uint64) raftpb.HardState {
	return raftpb.HardState{
		Term:   idx,
		Vote:   idx,
		Commit: idx,
	}
}
func buildEntries(idx uint64) []raftpb.Entry {
	entries := make([]raftpb.Entry, 0, idx)
	data := make([]byte, 1024)

	for i := uint64(0); i < idx; i++ {
		entries = append(entries, raftpb.Entry{
			Term:  i,
			Index: i,
			Type:  raftpb.EntryNormal,
			Data:  data,
		})
	}
	return entries
}

func TestSimpleWrite(t *testing.T) {
	walStore, err := NewWAL("test.wal", false)
	if err != nil {
		t.Fatal(err)
	}
	defer clean()
	state := buildState(1)
	entries := buildEntries(10)
	err = walStore.Save(state, entries)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSingleWriteLoop(t *testing.T) {
	walStore, err := NewWAL("test.wal", true)
	if err != nil {
		t.Fatal(err)
	}
	defer clean()
	for i := 0; i < 10000; i++ {
		state := buildState(uint64(i))
		entries := buildEntries(uint64(1))
		err = walStore.Save(state, entries)
		if err != nil {
			t.Fatal(err)
		}
	}
}
