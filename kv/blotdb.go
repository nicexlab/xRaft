package kv

import (
	"log"
	"time"

	"github.com/boltdb/bolt"
)

type BlotDb_KVStore struct {
	dbPath string
	db     *bolt.DB
}

func NewBlotdb(dbPath string) *BlotDb_KVStore {
	kv := &BlotDb_KVStore{dbPath: dbPath}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	kv.db = db
	return kv
}

// func (m *BlotDb_KVStore) Get(k Key) (Val, KvOpStatus) {

// 	// v, ok := m.kv[k]
// 	// if ok {
// 	// 	return v, SUCCEED
// 	// } else {
// 	// 	return v, KEY_NOT_EXIST
// 	// }

// }

// func (m *BlotDb_KVStore) Put(k Key, v Val) (Val, KvOpStatus) {
// 	old_val := m.kv[k]
// 	m.kv[k] = v
// 	return old_val, SUCCEED
// }

// func (m *BlotDb_KVStore) Delete(k Key) (Val, KvOpStatus) {
// 	old_val := m.kv[k]
// 	delete(m.kv, k)
// 	return old_val, SUCCEED
// }

// func (m *BlotDb_KVStore) RLock(k Key) (Val, KvOpStatus) {
// 	old_val := m.kv[k]
// 	// Rlock k
// 	return old_val, SUCCEED
// }

// func (m *BlotDb_KVStore) WLock(k Key) (Val, KvOpStatus) {
// 	old_val := m.kv[k]
// 	// Wlock k
// 	return old_val, SUCCEED
// }

// func (m *BlotDb_KVStore) Printf() {
// 	fmt.Printf("%v\n", m.kv)
// }

// func (m *BlotDb_KVStore) Equal(kv *BlotDb_KVStore) bool {
// 	for key, val1 := range m.kv {
// 		if val2, ok := m.kv[key]; !ok {
// 			return false
// 		} else {
// 			if val1 != val2 {
// 				return false
// 			}
// 		}
// 	}
// 	return true
// }

// func (m *BlotDb_KVStore) GetSnapshot() ([]byte, error) {
// 	return json.Marshal(m.kv)
// }

// func (m *BlotDb_KVStore) RecoverFromSnapshot(snapshot []byte) error {
// 	if err := json.Unmarshal(snapshot, &m.kv); err != nil {
// 		return err
// 	}
// 	return nil
// }
