package xraft

// type CommitHelper struct {
// 	Data       []string
// 	ApplyDoneC chan<- struct{}
// }

// type XStateType = int8

// const (
// 	RAFT XStateType = iota
// 	FAST
// 	MERGE
// )

// type FastPropose struct {
// 	FastTerm    int32
// 	ClientId    int32
// 	CommittedId int32
// 	CommandId   int32
// 	Command     pb.ClientCommand
// }

// type MergeTrigger struct {
// 	FastTerm int32
// }

// type ProposeReply struct {
// 	Status    pb.ReplyStatusCode
// 	CommandId int32
// 	Value     string
// }

// type FastCollectReply struct {
// 	FastTerm        int32
// 	ClientCommitted []int32
// 	CommandAccepted [][]int32
// 	CommandWaiting  [][]int32 // maybe rename a better one?
// }
