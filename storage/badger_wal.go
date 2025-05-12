package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

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

func (s *BadgerWAL) Append(entries []raftpb.Entry) error {
	for _, entry := range entries {
		key := encodeKey(&entry)
		err := s.db.Update(func(txn *badger.Txn) error {
			err := txn.Set(key, entry.Data)
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *BadgerWAL) SaveHardState(state raftpb.HardState) error {
	key := "state"
	err := s.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), []byte(state.String()))
		return err
	})
	return err
}

func (s *BadgerWAL) Save(state raftpb.HardState, entries []raftpb.Entry) error {
	err := s.SaveHardState(state)
	if err != nil {
		return err
	}
	err = s.Append(entries)
	return err

}

func encodeKey(entry *raftpb.Entry) []byte {
	key := fmt.Sprintf("%d-%d-%d", entry.Term, entry.Index, entry.Type)
	return []byte(key)
}
