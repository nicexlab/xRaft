package kv

type KvOpStatus uint8

const (
	SUCCEED KvOpStatus = iota
	KEY_NOT_EXIST
)

type KVStore interface {
	Get(k string) (string, error)
	Put(k string, v string) error
	Delete(k string) error
	PutWithOldValue(k string, v string) (string, error)
	RollbackPutWithOldValue(k, old_v string) error
	RollbackDeleteWithOldValue(k, old_v string) error
	DeleteWithOldValue(k string) (string, error)
	Destroy()
}
