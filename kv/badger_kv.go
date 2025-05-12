package kv

import (
	"os"

	"github.com/dgraph-io/badger/v4"
)

type BadgerStore struct {
	path string
	db   *badger.DB
}

func NewBadgerStore(dbPath string) (*BadgerStore, error) {
	db, err := badger.Open(badger.DefaultOptions(dbPath))
	store := &BadgerStore{
		path: dbPath,
		db:   db,
	}
	return store, err
}

func (s *BadgerStore) Close() {
	s.db.Close()
}

func (s *BadgerStore) Destroy() {
	s.db.Close()
	os.RemoveAll(s.path)
}

func (s *BadgerStore) Put(key string, value string) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), []byte(value))
		return err
	})
	return err
}

// Txn.Get() returns ErrKeyNotFound if the value is not found.
func (s *BadgerStore) Get(key string) (string, error) {
	var valCopy []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		valCopy, err = item.ValueCopy(nil)

		if err != nil {
			return err
		}
		return nil
	})
	return string(valCopy), err
}

func (s *BadgerStore) Delete(key string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

func (s *BadgerStore) PutWithOldValue(key string, value string) (string, error) {
	var old_value []byte
	var readErr error
	err := s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		// ignore this error ,
		readErr = err
		if readErr == nil {
			old_value, readErr = item.ValueCopy(nil)
		}
		return txn.Set([]byte(key), []byte(value))
	})
	if readErr != nil {
		return "", readErr
	}
	return string(old_value), err
}

func (s *BadgerStore) RollbackPutWithOldValue(key, old_value string) error {
	if len(old_value) == 0 {
		return s.Delete(key)
	} else {
		return s.Put(key, old_value)
	}
}

func (s *BadgerStore) RollbackDeleteWithOldValue(key, old_value string) error {
	return s.Put(key, old_value)
}

func (s *BadgerStore) DeleteWithOldValue(key string) (string, error) {
	var value []byte
	err := s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return txn.Delete([]byte(key))
	})

	return string(value), err
}
