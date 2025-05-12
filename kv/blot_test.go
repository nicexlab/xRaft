package kv

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestBoltPutGet(t *testing.T) {
	kv, err := NewBoltStore("test.db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Destroy()
	if err := kv.Put("key", "value"); err != nil {
		t.Fatal(err)
	}
	if v, err := kv.Get("key"); err != nil {
		t.Fatal(err)
	} else if string(v) != "value" {
		t.Fatal("value not match")
	}
}

func TestBoltRollBackWithOldValue(t *testing.T) {
	kv, err := NewBoltStore("test.db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Destroy()

	old_value1, _ := kv.PutWithOldValue("key", "value1")

	old_value2, _ := kv.PutWithOldValue("key", "value2")
	if v, err := kv.Get("key"); err != nil {
		t.Fatal(err)
	} else if string(v) != "value2" {
		t.Fatal("value not match")
	}
	// rollback

	kv.RollbackPutWithOldValue("key", old_value2)
	if v, err := kv.Get("key"); err != nil {
		t.Fatal(err)
	} else if string(v) != "value1" {
		t.Fatal("value not match")
	}

	kv.RollbackPutWithOldValue("key", old_value1)
	value, err := kv.Get("key")
	if err == nil {
		t.Log(value)
		t.Fatal("value should not exist")
	}

}

func TestBoltRollBackDeleteWithOldValue(t *testing.T) {
	kv, err := NewBoltStore("test.db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Destroy()

	kv.PutWithOldValue("key", "value")
	if v, err := kv.Get("key"); err != nil {
		t.Fatal(err)
	} else if string(v) != "value" {
		t.Fatal("value not match")
	}

	old_value, _ := kv.DeleteWithOldValue("key")
	value, err := kv.Get("key2")
	if err == nil {
		t.Log(value)
		t.Fatal("value should not exist")
	}

	kv.RollbackDeleteWithOldValue("key", old_value)
	if v, err := kv.Get("key"); err != nil {
		t.Fatal(err)
	} else if string(v) != "value" {
		t.Fatal("value not match")
	}
}

func TestBoltRollBackspeed(t *testing.T) {
	kv, err := NewBoltStore("test.db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Destroy()

	val := bytes.Repeat([]byte("hello"), 204)
	start := time.Now()
	for i := 0; i < 1000; i++ {
		kv.PutWithOldValue("key", string(val))
	}
	end := time.Now()
	fmt.Printf("used time %v\n", end.Sub(start).Milliseconds())
}

func TestBoltSimpleSpeed(t *testing.T) {
	kv, err := NewBoltStore("test.db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Destroy()

	val := bytes.Repeat([]byte("hello"), 204)
	start := time.Now()
	for i := 0; i < 1000; i++ {
		kv.Put("key", string(val))
	}
	end := time.Now()
	fmt.Printf("used time %v\n", end.Sub(start).Milliseconds())
}
