package conn

import (
	"fmt"
	"sync"
)

type fastLogs struct {
	logs []*ResultedCommand
	cfi  int
	mu   sync.Mutex
}

func NewFastLogs() *fastLogs {
	l := &fastLogs{logs: make([]*ResultedCommand, 0), cfi: -1, mu: sync.Mutex{}}
	return l
}

func (l *fastLogs) Append(rcmd *ResultedCommand) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.logs = append(l.logs, rcmd)
}

func (l *fastLogs) GetUncommittedTxs() (int, []*ResultedCommand) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for i := l.cfi + 1; i < len(l.logs); i++ {
		if !l.logs[i].committed {
			l.cfi = i - 1
			// log.Printf("set l.cfi to %v", l.cfi)
			return i, l.logs[i:] // 返回最后一段未提交的logs
		}
	}
	l.cfi = len(l.logs) - 1
	return len(l.logs), nil
}

func (l *fastLogs) GetUncommittedTail(deep int) (int, []*ResultedCommand) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for i := l.cfi + 1; i < len(l.logs); i++ {
		if !l.logs[i].committed {
			l.cfi = i - 1
			break
		}
	}
	if l.cfi-deep >= 0 {
		return l.cfi - deep + 1, l.logs[l.cfi-deep+1:]
	} else {
		return 0, l.logs[:]
	}
}

func (l *fastLogs) PersistenceKey() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.logs = make([]*ResultedCommand, 0)
	l.cfi = -1
}

func (l *fastLogs) FixUncommittedTxs(start int, txs []*ResultedCommand) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.logs) < start {
		return fmt.Errorf("not enough len to fix the fast logs from len %v (local %v)", start, len(l.logs))
	}
	l.logs = append(l.logs[0:start], txs...)
	return nil
}
