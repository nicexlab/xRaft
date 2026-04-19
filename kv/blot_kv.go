package kv

import (
	"fmt"
	"os"

	"github.com/boltdb/bolt"
)

type BoltStore struct {
	path       string
	db         *bolt.DB
	bucketName string
}

// NewBoltStore 创建并初始化 KVStore，包括初始化 bucket
func NewBoltStore(dbPath string, bucketName string) (*BoltStore, error) {
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
	store := &BoltStore{path: dbPath, db: db, bucketName: bucketName}

	if err != nil {
		db.Close()
		return nil, err
	}

	return store, nil
}

// Close conn
func (s *BoltStore) Close() {
	s.db.Close()
}

// Close conn and remove db file
func (s *BoltStore) Destroy() {
	s.db.Close()
	os.Remove(s.path)

}

// begin a new transaction and put
func (s *BoltStore) Put(key string, value string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(s.bucketName))
		return bucket.Put([]byte(key), []byte(value))
	})
}

// begin a new transaction and get
func (s *BoltStore) Get(key string) (string, error) {
	var value []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(s.bucketName))
		value = bucket.Get([]byte(key))
		if value == nil {
			return bolt.ErrBucketNotFound
		}
		value = append([]byte{}, value...) // Copy value to avoid referencing the memory-mapped file
		return nil
	})
	return string(value), err
}

// begin a new transaction and delete
func (s *BoltStore) Delete(key string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(s.bucketName))
		return bucket.Delete([]byte(key))
	})
}

func (s *BoltStore) PutWithOldValue(key string, value string) (string, error) {
	var oldValue []byte
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(s.bucketName))
		oldValue = bucket.Get([]byte(key))
		return bucket.Put([]byte(key), []byte(value))
	})
	if oldValue == nil {
		return "", err
	}
	return string(oldValue), err
}

func (s *BoltStore) RollbackPutWithOldValue(key string, oldValue string) error {
	if len(oldValue) == 0 {
		return s.Delete(key)
	}
	return s.Put(key, oldValue)
}

func (s *BoltStore) RollbackDeleteWithOldValue(key string, oldValue string) error {
	return s.Put(key, oldValue)
}

func (s *BoltStore) DeleteWithOldValue(key string) (string, error) {
	var value []byte
	err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(s.bucketName))
		value = bucket.Get([]byte(key))
		return bucket.Delete([]byte(key))
	})
	return string(value), err
}

func (s *BoltStore) Equal(t *BoltStore) error {
	keys := make([]string, 0)
	values := make([]string, 0)

	s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(s.bucketName))
		c := bucket.Cursor()
		for key, _ := c.First(); key != nil; key, _ = c.Next() {
			keys = append(keys, string(key))
			value := bucket.Get([]byte(key))
			if value == nil {
				return bolt.ErrBucketNotFound
			} else {
				values = append(values, string(value))
			}
		} // Copy value to avoid referencing the memory-mapped file
		return nil
	})

	err := t.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(s.bucketName))
		c := bucket.Cursor()
		for key, _ := c.First(); key != nil; key, _ = c.Next() {
			value := bucket.Get([]byte(key))

			index := -1
			for i := range keys {
				if keys[i] == string(key) {
					index = i
				}
			}
			if index == -1 {
				return fmt.Errorf("find key %v no exist", string(key))
			} else if values[index] != string(value) {
				return fmt.Errorf("find value at %v not equal, expect %v, get %v", string(key), string(value), values[index])
			}
		} // Copy value to avoid referencing the memory-mapped file
		return nil
	})
	return err
}

func (s *BoltStore) Begin() (*bolt.Tx, error) {
	tx, err := s.db.Begin(true)
	return tx, err

}

func (s *BoltStore) Commit(tx *bolt.Tx) error {
	err := tx.Commit()
	return err
}

func (s *BoltStore) Rollback(tx *bolt.Tx) error {
	err := tx.Rollback()
	return err
}
