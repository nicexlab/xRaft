package witness

import (
	"github/Fischer0522/xraft/curp/command"
	"github/Fischer0522/xraft/curp/curp_proto"
	"sync"
)

type ProposeMap = map[command.ProposeId]*curp_proto.CurpClientCommand
type Witness struct {
	mu         sync.Mutex
	commandMap map[string]ProposeMap
}

func NewWitness() Witness {
	return Witness{
		commandMap: make(map[string]map[command.ProposeId]*curp_proto.CurpClientCommand),
	}
}

func (w *Witness) InsertIfNotConflict(cmd *curp_proto.CurpClientCommand) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	// command exists before report conflict
	if w.HasConflictWith(cmd) {
		return true
	}
	record_map, ok := w.commandMap[cmd.Key]
	// create a new record map if not exists
	if !ok {
		record_map = make(map[command.ProposeId]*curp_proto.CurpClientCommand)
	}
	proposeId := cmd.ProposeId()
	record_map[proposeId] = cmd
	w.commandMap[cmd.Key] = record_map

	return false
}

func (w *Witness) Remove(cmd *curp_proto.CurpClientCommand) {
	w.mu.Lock()
	defer w.mu.Unlock()
	key := cmd.Key
	record_map, ok := w.commandMap[key]

	if !ok {
		return
	}

	delete(record_map, cmd.ProposeId())
	if len(record_map) == 0 {
		delete(w.commandMap, key)
	}
}

func (w *Witness) HasConflictWith(cmd *curp_proto.CurpClientCommand) bool {

	record_set, ok := w.commandMap[cmd.Key]
	if !ok {
		return false
	}
	for _, recordedCmd := range record_set {
		if curp_proto.Conflict(recordedCmd, cmd) {
			return true
		}
	}
	return false
}
