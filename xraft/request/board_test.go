package request

import (
	"sync"
	"testing"
)

func TestValue(t *testing.T) {
	b := NewBoard()
	id := RequestID{ClientID: 1, SeqID: 1}
	b.InsertMr(id, "result")

	v := b.WaitForMr(id)

	if v.(string) != "result" {
		t.Errorf("vale error")
	}
}

func TestCurrent(t *testing.T) {
	b := NewBoard()
	num := 1000000
	wg := sync.WaitGroup{}
	wg.Add(num)
	for i := 0; i < num; i++ {
		go func(id int) {
			r := RequestID{ClientID: uint64(id), SeqID: uint64(id)}
			w := RequestID{ClientID: (uint64(id) + 1) % uint64(num), SeqID: (uint64(id) + 1) % uint64(num)}

			b.InsertMr(r, id)
			b.WaitForMr(w)
			wg.Done()
		}(i)
	}
	wg.Wait()
}
