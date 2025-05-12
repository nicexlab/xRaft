package curp

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"testing"
)

func TestClientSimple(t *testing.T) {
	defer cleanPersist()
	cluster := newCluster(3)
	waitElection()

	client, err := NewClient(cluster.state, 1)
	if err != nil {
		t.Fatal(err)
	}
	key, value := "test_key", "test_value"
	err = client.Put(key, value)
	if err != nil {
		t.Fatal(err)
	}

	result, err := client.Get(key)

	if err != nil {
		t.Fatal(err)
	}
	if result != value {
		t.Fatal("value not match")
	}

}

func TestClientMulti(t *testing.T) {
	defer cleanPersist()
	cluster := newCluster(3)
	waitElection()

	client, err := NewClient(cluster.state, 1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		key, value := fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i)
		err = client.Put(key, value)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 1000; i++ {
		key, value := fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i)
		result, err := client.Get(key)

		if err != nil {
			t.Error(err)
			t.Fatal(err)
		}
		if result != value {
			t.Errorf("value not match,expected: %s,got: %s", value, result)
			t.Fatal()
		}
	}
}

func TestClientAllConflict(t *testing.T) {
	f, _ := os.OpenFile("cpu.pprof", os.O_CREATE|os.O_RDWR, 0644)
	defer f.Close()
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(1)
	defer cleanPersist()
	cluster := newCluster(3)
	waitElection()

	client, err := NewClient(cluster.state, 1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		key, value := fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i)
		err = client.Put(key, value)
		if err != nil {
			t.Fatal(err)
		}
		result, err := client.Get(key)

		if err != nil {
			t.Error(err)
			t.Fatal(err)
		}
		if result != value {
			t.Errorf("value not match,expected: %s,got: %s", value, result)
			t.Fatal()
		}
	}

	// 在程序结束前，将profile结果写入文件
	f1, err := os.Create("block.pprof")
	if err != nil {
		panic(err)
	}
	defer f1.Close()
	pprof.Lookup("block").WriteTo(f1, 0)

	if f2, err := os.Create("mutex.pprof"); err == nil {
		defer f2.Close()
		pprof.Lookup("mutex").WriteTo(f2, 0)
	}

}

func TestClientNoConflict(t *testing.T) {
	defer cleanPersist()
	cluster := newCluster(3)
	waitElection()

	client, err := NewClient(cluster.state, 1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		key, value := fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i)
		err = client.Put(key, value)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestClientCurrent(t *testing.T) {
	defer cleanPersist()
	cluster := newCluster(3)
	waitElection()
	var wg sync.WaitGroup
	wg.Add(3)
	client1, err := NewClient(cluster.state, 1)
	if err != nil {
		t.Fatal(err)
	}
	client2, err := NewClient(cluster.state, 2)
	if err != nil {
		t.Fatal(err)
	}
	client3, err := NewClient(cluster.state, 3)
	if err != nil {
		t.Fatal(err)
	}
	go func(clientId int) {
		for i := 0; i < 500; i++ {
			key, value := fmt.Sprintf("key-%d-%d", clientId, i), fmt.Sprintf("value-%d", i)
			err = client1.Put(key, value)
			if err != nil {
				t.Fail()
			}
			result, err := client1.Get(key)

			if err != nil {
				t.Error(err)
				t.Fail()
			}
			if result != value {
				t.Errorf("value not match,expected: %s,got: %s", value, result)
				t.Fail()
			}
		}
		wg.Done()
	}(1)
	go func(clientId int) {
		for i := 0; i < 500; i++ {
			key, value := fmt.Sprintf("key-%d-%d", clientId, i), fmt.Sprintf("value-%d", i)
			err = client2.Put(key, value)
			if err != nil {
				t.Fail()
			}
			result, err := client2.Get(key)

			if err != nil {
				t.Error(err)
				t.Fail()
			}
			if result != value {
				t.Errorf("value not match,expected: %s,got: %s", value, result)
				t.Fail()
			}
		}
		wg.Done()
	}(2)
	go func(clientId int) {
		for i := 0; i < 500; i++ {
			key, value := fmt.Sprintf("key-%d-%d", clientId, i), fmt.Sprintf("value-%d", i)
			err = client3.Put(key, value)
			if err != nil {
				t.Fail()
			}
		}
		wg.Done()
	}(3)

	wg.Wait()

}

func TestClientCurrentConflict(t *testing.T) {
	defer cleanPersist()
	cluster := newCluster(3)
	waitElection()
	var wg sync.WaitGroup
	wg.Add(3)
	client1, err := NewClient(cluster.state, 1)
	if err != nil {
		t.Fatal(err)
	}
	client2, err := NewClient(cluster.state, 2)
	if err != nil {
		t.Fatal(err)
	}
	client3, err := NewClient(cluster.state, 3)
	if err != nil {
		t.Fatal(err)
	}
	go func(clientId int) {
		for i := 0; i < 1500; i++ {
			key, value := fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i)
			err = client1.Put(key, value)
			if err != nil {
				log.Println(err)
				t.Failed()
			}

		}
		wg.Done()
	}(1)
	go func(clientId int) {
		for i := 0; i < 1500; i++ {
			key, value := fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i)
			err = client2.Put(key, value)
			if err != nil {
				log.Println(err)
				t.Failed()
			}
		}
		wg.Done()
	}(2)
	go func(clientId int) {
		for i := 0; i < 1500; i++ {
			key, value := fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i)
			err = client3.Put(key, value)
			if err != nil {
				log.Println(err)
				t.Failed()
			}
		}
		wg.Done()
	}(3)

	wg.Wait()

	for i := 0; i < 1500; i++ {
		key, value := fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i)
		reply, err := client3.Get(key)
		if err != nil {
			log.Println(err)
			t.Failed()
		}

		if reply != value {
			log.Println(err)
			t.Failed()
		}
	}

}
