package xraft

import (
	"math/rand"
)

// 生成随机字符串
func RandomString(length int, rand *rand.Rand) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// func CompareMessages(msg1, msg2 *xproto.Message) bool {
// 	// 比较 Nonce 字段
// 	if msg1.Nonce != msg2.Nonce {
// 		return false
// 	}

// 	// 比较 PrepareTxs 字段
// 	if !compareTransactions(msg1.PrepareTxs, msg2.PrepareTxs) {
// 		return false
// 	}

// 	// 比较 CommitTxs 字段
// 	if !compareTransactionNonces(msg1.CommitTxs, msg2.CommitTxs) {
// 		return false
// 	}

// 	// 比较 AbortTxs 字段
// 	if !compareTransactionNonces(msg1.AbortTxs, msg2.AbortTxs) {
// 		return false
// 	}

// 	return true
// }

// // 比较 Transactions 字段
// func compareTransactions(tx1, tx2 *xproto.Transactions) bool {
// 	if len(tx1.Trans) != len(tx2.Trans) {
// 		return false
// 	}

// 	for i := 0; i < len(tx1.Trans); i++ {
// 		if !proto.Equal(tx1.Trans[i], tx2.Trans[i]) {
// 			return false
// 		}
// 	}

// 	return true
// }

// // 比较 TransactionNonces 字段
// func compareTransactionNonces(txns1, txns2 *xproto.TransactionNonces) bool {
// 	if len(txns1.Nonce) != len(txns2.Nonce) {
// 		return false
// 	}

// 	for i := 0; i < len(txns1.Nonce); i++ {
// 		if txns1.Nonce[i] != txns2.Nonce[i] {
// 			return false
// 		}
// 	}

// 	return true
// }

// func GenerateRandomMessageReply(rand *rand.Rand) (*xproto.MessageReply, error) {
// 	// 初始化随机数生成器
// 	// rand.Seed(time.Now().UnixNano())

// 	// 生成随机的 TransactionReply
// 	numPrepareTxs := rand.Intn(10) + 1 // 随机生成 1 到 10 个 PrepareTxs
// 	ptxs := make([]*xproto.TransactionReply, numPrepareTxs)
// 	for i := 0; i < numPrepareTxs; i++ {
// 		ptx := &xproto.TransactionReply{
// 			Nonce: uint64(rand.Uint32()),
// 			R:     uint32(rand.Intn(100)),
// 			Val:   RandomString(10, rand),
// 		}
// 		ptxs[i] = ptx
// 	}

// 	numCommitTxs := rand.Intn(10) + 1 // 随机生成 1 到 10 个 PrepareTxs
// 	ptxs2 := make([]*xproto.TransactionReply, numCommitTxs)
// 	for i := 0; i < numCommitTxs; i++ {
// 		ptx := &xproto.TransactionReply{
// 			Nonce: uint64(rand.Uint32()),
// 			R:     uint32(rand.Intn(100)),
// 			Val:   RandomString(10, rand),
// 		}
// 		ptxs2[i] = ptx
// 	}

// 	numAbortTxs := rand.Intn(10) + 1 // 随机生成 1 到 10 个 PrepareTxs
// 	ptxs3 := make([]*xproto.TransactionReply, numAbortTxs)
// 	for i := 0; i < numAbortTxs; i++ {
// 		ptx := &xproto.TransactionReply{
// 			Nonce: uint64(rand.Uint32()),
// 			R:     uint32(rand.Intn(100)),
// 			Val:   RandomString(10, rand),
// 		}
// 		ptxs3[i] = ptx
// 	}

// 	numSlowTxs := rand.Intn(10) + 1 // 随机生成 1 到 10 个 PrepareTxs
// 	ptxs4 := make([]*xproto.TransactionReply, numSlowTxs)
// 	for i := 0; i < numSlowTxs; i++ {
// 		ptx := &xproto.TransactionReply{
// 			Nonce: uint64(rand.Uint32()),
// 			R:     uint32(rand.Intn(100)),
// 			Val:   RandomString(10, rand),
// 		}
// 		ptxs4[i] = ptx
// 	}

// 	// 构建 conn.Message 结构体
// 	message := &xproto.MessageReply{
// 		Nonce:      rand.Uint64(),
// 		State:      rand.Uint32(),
// 		PrepareTxs: ptxs,
// 		CommitTxs:  ptxs2,
// 		AbortTxs:   ptxs3,
// 		SlowTxs:    ptxs4,
// 	}

// 	return message, nil
// }

// func CompareMessageReplies(reply1, reply2 *xproto.MessageReply) bool {
// 	// 比较 Nonce 字段
// 	if reply1.Nonce != reply2.Nonce {
// 		return false
// 	}

// 	// 比较 State 字段
// 	if reply1.State != reply2.State {
// 		return false
// 	}

// 	// 比较 PrepareTxs 字段
// 	if !compareTransactionReplies(reply1.PrepareTxs, reply2.PrepareTxs) {
// 		return false
// 	}

// 	// 比较 CommitTxs 字段
// 	if !compareTransactionReplies(reply1.CommitTxs, reply2.CommitTxs) {
// 		return false
// 	}

// 	// 比较 AbortTxs 字段
// 	if !compareTransactionReplies(reply1.AbortTxs, reply2.AbortTxs) {
// 		return false
// 	}

// 	// 比较 SlowTxs 字段
// 	if !compareTransactionReplies(reply1.SlowTxs, reply2.SlowTxs) {
// 		return false
// 	}

// 	return true
// }

// // 比较 TransactionReply 数组
// func compareTransactionReplies(tx1, tx2 []*xproto.TransactionReply) bool {
// 	if len(tx1) != len(tx2) {
// 		return false
// 	}

// 	for i := 0; i < len(tx1); i++ {
// 		if !proto.Equal(tx1[i], tx2[i]) {
// 			return false
// 		}
// 	}

// 	return true
// }
