package kv

import (
	"encoding/json"
	"fmt"
	"sync"
)

// thread safe memory kv
type MemStore struct {
	kv map[string]string
	mu *sync.RWMutex
}

func NewMemStore() (*MemStore, error) {
	mkv := &MemStore{}
	mkv.kv = make(map[string]string)
	mkv.mu = &sync.RWMutex{}
	return mkv, nil
}

func (m *MemStore) Get(k string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.kv[k]
	if ok {
		return v, nil
	} else {
		return v, fmt.Errorf("key not found")
	}
}

func (m *MemStore) Put(k string, v string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kv[k] = v
	return nil
}

func (m *MemStore) Delete(k string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.kv, k)
	return nil
}

func (m *MemStore) PutWithOldValue(k string, v string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old_v, ok := m.kv[k]
	m.kv[k] = v
	if ok {
		return old_v, nil
	} else {
		return "", fmt.Errorf("key not found")
	}
}

func (m *MemStore) RollbackPutWithOldValue(k, old_v string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if old_v == "" {
		delete(m.kv, k)
	} else {
		m.kv[k] = old_v
	}
	return nil
}

func (m *MemStore) RollbackDeleteWithOldValue(k, old_v string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kv[k] = old_v
	return nil
}

func (m *MemStore) DeleteWithOldValue(k string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	old_v, ok := m.kv[k]
	delete(m.kv, k)
	if ok {
		return old_v, nil
	} else {
		return "", fmt.Errorf("key not found")
	}
}
func (m *MemStore) Destroy() {
	m.kv = make(map[string]string)
}

// func (m *Mem_kvStore) RLock(k string) (string, KvOpStatus) {
// 	old_val := m.kv[k]
// 	// Rlock k
// 	return old_val, SUCCEED
// }

// func (m *Mem_kvStore) WLock(k string) (string, KvOpStatus) {
// 	old_val := m.kv[k]
// 	// Wlock k
// 	return old_val, SUCCEED
// }

func (m *MemStore) Printf() {
	fmt.Printf("%v\n", m.kv)
}

func (m *MemStore) Equal(kv *MemStore) bool {
	for key, val1 := range m.kv {
		if val2, ok := m.kv[key]; !ok {
			return false
		} else {
			if val1 != val2 {
				return false
			}
		}
	}
	return true
}

func (m *MemStore) GetSnapshot() ([]byte, error) {
	return json.Marshal(m.kv)
}

func (m *MemStore) RecoverFromSnapshot(snapshot []byte) error {
	if err := json.Unmarshal(snapshot, &m.kv); err != nil {
		return err
	}
	return nil
}
