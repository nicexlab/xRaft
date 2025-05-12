package storage

import (
	"bufio"
	"encoding/binary"
	"os"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

type RecordType uint16

const (
	STATE_TYPE RecordType = iota
	ENTRY_TYPE
)

type WAL struct {
	file    *os.File
	writer  *bufio.Writer
	logPath string
	sync    bool
}

// NewWAL 创建一个新的WAL实例
func NewWAL(logPath string, Sync bool) (*WAL, error) {
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file:    file,
		writer:  bufio.NewWriter(file),
		logPath: logPath,
	}, nil
}

func (wal *WAL) Save(hardState raftpb.HardState, entries []raftpb.Entry) error {
	record := make([]byte, 0)
	record = append(record, byte(STATE_TYPE))
	record = append(record, *encodeHardState(hardState)...)
	record = append(record, byte(ENTRY_TYPE))
	record = append(record, *encodeEntries(entries)...)
	_, err := wal.writer.Write(record)
	if err != nil {
		return err
	}
	if wal.sync {
		err = wal.writer.Flush()
	}
	return err
}

// func (wal *WAL) Replay() error {
// 	offset := int64(0)

// 	state := raftpb.HardState{}
// 	stat, err := wal.file.Stat()
// 	if err != nil {
// 		return err
// 	}

// 	for offset < stat.Size() {

// 	}
// 	return nil
// }

func encodeHardState(hardState raftpb.HardState) *[]byte {
	// three element, term, vote, commit each 8 bytes,represent Uint64
	buffer := make([]byte, 0, 8*3)
	buffer = binary.BigEndian.AppendUint64(buffer, hardState.Term)
	buffer = binary.BigEndian.AppendUint64(buffer, hardState.Vote)
	buffer = binary.BigEndian.AppendUint64(buffer, hardState.Commit)
	return &buffer
}

func encodeEntries(entries []raftpb.Entry) *[]byte {
	// four element, term, index, type, data
	buffer := make([]byte, 0)
	for _, entry := range entries {
		buffer = binary.BigEndian.AppendUint64(buffer, entry.Term)
		buffer = binary.BigEndian.AppendUint64(buffer, entry.Index)
		buffer = binary.BigEndian.AppendUint16(buffer, uint16(entry.Type))
		buffer = binary.BigEndian.AppendUint64(buffer, uint64(len(entry.Data)))
		buffer = append(buffer, entry.Data...)
	}
	return &buffer
}

// Close 关闭WAL文件
func (wal *WAL) Close() error {
	if err := wal.writer.Flush(); err != nil {
		return err
	}
	return wal.file.Close()
}
