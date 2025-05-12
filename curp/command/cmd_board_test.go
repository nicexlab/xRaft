package command

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"
)

func TestWaitEr(t *testing.T) {

	expected := "ast"
	cmd_board := NewBoard()
	proposeId := ProposeId{
		ClientId: 1,
		SeqId:    1,
	}
	go func() {
		time.Sleep(2 * time.Second)

		cmd_board.InsertEr(proposeId, "ast")
	}()
	result := cmd_board.WaitForEr(proposeId)
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

// this case should never happen,but just test it
func TestInsertBeforeWait(t *testing.T) {
	expected := "val1"
	cmd_board := NewBoard()
	proposeId := ProposeId{
		ClientId: 1,
		SeqId:    1,
	}
	go func() {
		cmd_board.InsertEr(proposeId, "val1")
	}()
	time.Sleep(2 * time.Second)
	result := cmd_board.WaitForEr(proposeId)
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestWaitAsr(t *testing.T) {
	expected := "val1"
	cmd_board := NewBoard()
	proposeId := ProposeId{
		ClientId: 1,
		SeqId:    1,
	}
	// we should't call this func in real situation,just for test
	cmd_board.InsertEr(proposeId, expected)
	go func() {
		time.Sleep(2 * time.Second)
		cmd_board.NotifyAsr(proposeId)
	}()
	result := cmd_board.WaitForAsr(proposeId)
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

// this might happen,the execution of command might be earlier than the wait synced func
func TestInsertBeforeWaitAsr(t *testing.T) {
	expected := "val1"
	cmd_board := NewBoard()
	proposeId := ProposeId{
		ClientId: 1,
		SeqId:    1,
	}

	cmd_board.InsertEr(proposeId, expected)
	go func() {
		cmd_board.NotifyAsr(proposeId)
	}()
	time.Sleep(2 * time.Second)
	result := cmd_board.WaitForAsr(proposeId)
	if result != expected {
		t.Errorf("Expected %s,got %s", expected, result)
	}
}

func TestConcurrentEr(t *testing.T) {
	var wg sync.WaitGroup

	cmd_board := NewBoard()
	for i := 0; i < 10; i++ {
		proposeId := ProposeId{
			ClientId: uint64(i),
			SeqId:    uint64(i),
		}
		go func(i int) {
			cmd_board.InsertEr(proposeId, fmt.Sprintf("val%d", i))
		}(i)
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		proposeId := ProposeId{
			ClientId: uint64(i),
			SeqId:    uint64(i),
		}
		go func(i int) {
			defer wg.Done()

			result := cmd_board.WaitForEr(proposeId)
			expected := fmt.Sprintf("val%d", i)
			log.Printf("got result: %s", result)
			if result != expected {
				t.Errorf("Expected %s, got %s", expected, result)
			}

		}(i)
	}
	wg.Wait()
}

func TestConcurrentAsr(t *testing.T) {
	var wg sync.WaitGroup
	cmd_board := NewBoard()
	for i := 0; i < 10; i++ {
		proposeId := ProposeId{
			ClientId: uint64(i),
			SeqId:    uint64(i),
		}
		cmd_board.InsertEr(proposeId, fmt.Sprintf("val%d", i))
		go func(i int) {
			if i%2 == 0 {
				time.Sleep(2 * time.Second)
			}
			cmd_board.NotifyAsr(proposeId)
		}(i)
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		proposeId := ProposeId{
			ClientId: uint64(i),
			SeqId:    uint64(i),
		}
		go func(i int) {
			defer wg.Done()
			result := cmd_board.WaitForAsr(proposeId)
			expected := fmt.Sprintf("val%d", i)
			log.Printf("got result: %s", result)
			if result != expected {
				t.Errorf("Expected %s, got %s", expected, result)
			}
		}(i)
	}
	wg.Wait()
}

// func TestMix(t *testing.T) {
// 	var wg sync.WaitGroup
// 	cmd_board := NewBoard()
// 	for i := 0; i < 10; i++ {

// 		go func(i int) {
// 			if i%2 == 0 {
// 				time.Sleep(2 * time.Second)
// 			}
// 			for j := 0; j < 1000; j++ {
// 				proposeId := ProposeId{
// 					ClientId: uint64(i * 10000 * j),
// 					SeqId:    uint64(i * 10000 * j),
// 				}
// 				cmd_board.InsertAsr(proposeId, fmt.Sprintf("val%d", i*10000+j))
// 			}

// 		}(i)
// 	}

// 	for i := 0; i < 10; i++ {
// 		go func(i int) {
// 			for j := 0; j < 1000; j++ {
// 				proposeId := ProposeId{
// 					ClientId: uint64(i * 10000 * j),
// 					SeqId:    uint64(i * 10000 * j),
// 				}
// 				cmd_board.InsertEr(proposeId, fmt.Sprintf("val%d", i*10000+j))
// 			}

// 		}(i)
// 	}
// 	for i := 0; i < 10; i++ {
// 		wg.Add(1)
// 		go func(i int) {
// 			defer wg.Done()
// 			for j := 0; j < 1000; j++ {
// 				proposeId := ProposeId{
// 					ClientId: uint64(i * 10000 * j),
// 					SeqId:    uint64(i * 10000 * j),
// 				}
// 				result := cmd_board.WaitForAsr(proposeId)
// 				expected := fmt.Sprintf("val%d", i*10000+j)
// 				log.Printf("got result: %s", result)
// 				if result != expected {
// 					t.Errorf("Expected %s, got %s", expected, result)
// 				}
// 			}
// 		}(i)
// 	}

// 	for i := 0; i < 10; i++ {
// 		wg.Add(1)
// 		go func(i int) {
// 			defer wg.Done()
// 			for j := 0; j < 1000; j++ {
// 				proposeId := ProposeId{
// 					ClientId: uint64(i * 10000 * j),
// 					SeqId:    uint64(i * 10000 * j),
// 				}
// 				result := cmd_board.WaitForEr(proposeId)
// 				expected := fmt.Sprintf("val%d", i*10000+j)
// 				log.Printf("got result: %s", result)
// 				if result != expected {
// 					t.Errorf("Expected %s, got %s", expected, result)
// 				}
// 			}
// 		}(i)
// 	}
// 	wg.Wait()
// }
