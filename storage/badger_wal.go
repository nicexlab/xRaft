package storage

import (
	"fmt"
	"os"

	"github.com/dgraph-io/badger/v4"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const badgerWALStateKey = "state"

type BadgerWAL struct {
	path string
	db   *badger.DB
}

func NewBadgerWAL(dbPath string) (*BadgerWAL, error) {
	options := badger.DefaultOptions(dbPath)
	options.SyncWrites = true
	db, err := badger.Open(options)
	if err != nil {
		return nil, err
	}
	store := &BadgerWAL{
		path: dbPath,
		db:   db,
	}
	return store, nil
}

func (s *BadgerWAL) Close() error {
	return s.db.Close()
}

func (s *BadgerWAL) Destroy() error {
	if err := s.Close(); err != nil {
		return err
	}
	return os.RemoveAll(s.path)
}

func (s *BadgerWAL) Append(entries []raftpb.Entry) error {
	for _, entry := range entries {
		key := encodeKey(&entry)
		if err := s.db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, entry.Data)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *BadgerWAL) SaveHardState(state raftpb.HardState) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(badgerWALStateKey), []byte(state.String()))
	})
}

func (s *BadgerWAL) Save(state raftpb.HardState, entries []raftpb.Entry) error {
	if err := s.SaveHardState(state); err != nil {
		return err
	}
	return s.Append(entries)
}

func encodeKey(entry *raftpb.Entry) []byte {
	key := fmt.Sprintf("%d-%d-%d", entry.Term, entry.Index, entry.Type)
	return []byte(key)
}
