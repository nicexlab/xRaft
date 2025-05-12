package conn

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type keystate uint8

const (
	fast keystate = iota
	merge
	slow
)

type rculock struct {
	rwmu        sync.RWMutex
	isWriting   atomic.Bool
	waitWriting chan struct{}
}

// 管理什么时候将key放入快速路径上
type stateManager struct {
	currentState keystate
	fterm        uint32
	blocked      bool
	lock         rculock
}

// func (k *stateManager) currentSlow() bool {
// 	return k.currentState == slow
// }

// func (k *stateManager) currentFast() bool {
// 	return k.currentState == fast
// }

func (k *rculock) RCURLock() {
	if k.isWriting.Load() { // 当前正在写，则需要等待写完成之后才能
		<-k.waitWriting
	}
	k.rwmu.RLock()
}

func (k *rculock) RCURUnlock() {
	k.rwmu.RUnlock()
}

func (k *rculock) RCUWLock() {
	k.isWriting.Store(true)
	k.rwmu.Lock()
}

func (k *rculock) RCUWUnlock() {
	k.isWriting.Store(false)
	close(k.waitWriting)
	k.waitWriting = make(chan struct{})
	k.rwmu.Unlock()
}

func (k *stateManager) fastLiveLock() bool {
	k.lock.RCURLock()
	return k.currentState == fast
}

func (k *stateManager) fastLiveUnlock() {
	k.lock.RCURUnlock()
}

// protected by k.rwmu.RLock()
func (k *stateManager) isBlocked() bool {
	return k.blocked
}

func (k *stateManager) getCurrentTerm() uint32 {
	return k.fterm
}

// fast -> merge -> slow -> fast
func (k *stateManager) setSlow() bool {
	k.lock.RCUWLock()
	defer k.lock.RCUWUnlock()
	if k.currentState != merge { // 只能从slow转为fast模式
		return false
	}
	k.currentState = slow
	return true
}

func (k *stateManager) canSetFast(term uint32) (bool, error) {
	k.lock.RCUWLock()
	defer k.lock.RCUWUnlock()

	if k.currentState != slow { // 只能从slow转为fast模式
		return false, nil
	}

	if term != k.fterm+1 {
		return false, fmt.Errorf("term error %v, want %v", term, k.fterm+1)
	}
	return true, nil
}

func (k *stateManager) setFastAndUnblocked(term uint32) (bool, error) {
	k.lock.RCUWLock()
	defer k.lock.RCUWUnlock()

	// if k.currentState != slow { // 只能从slow转为fast模式
	// 	return false, nil
	// }

	// if term != k.fterm+1 {
	// 	return false, fmt.Errorf("term error %v, want %v", term, k.fterm+1)
	// }

	k.currentState = fast
	k.fterm = term
	k.blocked = false
	return true, nil
}

func (k *stateManager) setBlocked() {
	k.lock.RCUWLock()
	defer k.lock.RCUWUnlock()

	k.blocked = true
}

func (k *stateManager) setMerge(term uint32) (bool, error) {
	k.lock.RCUWLock()
	defer k.lock.RCUWUnlock()

	if k.currentState != fast {
		return false, nil
	}
	if term != k.fterm {
		return false, fmt.Errorf("term error %v, want %v", term, k.fterm+1)
	}
	k.currentState = merge
	return true, nil
}

func newStateManager() *stateManager {
	k := stateManager{
		currentState: fast,
		fterm:        0,
		lock:         rculock{rwmu: sync.RWMutex{}, isWriting: atomic.Bool{}, waitWriting: make(chan struct{})},
		// slowKeys:        make(map[string]*time.Time),
		// scanner:         time.NewTicker(time.Millisecond * 600),
		// currentKeyState: make(map[string]keystate),

	}
	k.lock.isWriting.Store(false)
	// go k.backend(prepareFastC)
	return &k
}

// func (k *stateManager) setPrepareFast(key string) {
// 	t := time.Now()
// 	k.mu.Lock()
// 	k.slowKeys[key] = &t
// 	k.mu.Unlock()
// }

// func (k *stateManager) visitKey(key string) {
// 	t := time.Now()
// 	k.mu.Lock()
// 	if _, ok := k.slowKeys[key]; ok {
// 		k.slowKeys[key] = &t
// 	}
// 	k.mu.Unlock()
// }

// func (k *stateManager) backend(prepareFastC chan string) {
// 	for {
// 		_, ok := <-k.scanner.C
// 		if !ok {
// 			break
// 		}
// 		prepareFastKey := make([]string, 0)
// 		now := time.Now()
// 		k.mu.Lock()
// 		for key, r := range k.slowKeys {
// 			if now.Sub(*r) > time.Millisecond*600 {
// 				prepareFastKey = append(prepareFastKey, key)
// 				delete(k.slowKeys, key)
// 			}
// 		}
// 		k.mu.Unlock()
// 		for _, key := range prepareFastKey {
// 			prepareFastC <- key
// 		}
// 	}
// }

// type keyPathManager struct {
// 	currentSlowKey map[string]struct{}        // 当前处于slowpath的key, value表示当前有多少个key正在被处理
// 	slowSlot       map[string][]*Slow_Command // 当发生冲突时，顺序保存那些投机执行的命令，根据这个slot来进行交易的回滚
// 	blockPath      map[string]struct{}        // 当前拒绝fast path and slow path的key
// 	fastPath       map[string]int             // 当前key的fastpath的commit的数量
// }

// func newKeyPathManager() *keyPathManager {
// 	kpm := keyPathManager{
// 		currentSlowKey: make(map[string]struct{}),
// 		slowSlot:       make(map[string][]*Slow_Command),
// 		blockPath:      make(map[string]struct{}),
// 	}
// 	return &kpm
// }

// func (m *keyPathManager) keyIsSlow(key string) bool {
// 	_, ok := m.currentSlowKey[key]
// 	return ok
// }

// func (m *keyPathManager) setSlow(key string) {
// 	m.currentSlowKey[key] = struct{}{}
// }

// func (m *keyPathManager) sepculateCmds(key string) ([]*Slow_Command, bool) {
// 	_, ok := m.currentSlowKey[key]
// 	return ok
// }

// type clientInfo struct {
// 	clientCommittedKey  map[Addr]map[string][]uint64 // 记录client提交关于某个key的交易
// 	clientCommittedcmds map[Addr]map[uint64]struct{} // 记录client提交的交易
// }
