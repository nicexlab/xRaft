package pb

type ReplyStatusCode uint8

const (
	SLOW_SUCCEED ReplyStatusCode = iota

	CURRENT_PREAPRE_FAST // 只有 leader 才会回复这个state
	CURRENT_SLOWMODE     // 只有follower会回复这个state

	BACATCH_WHEN_BLOCK

	BATCHED_FAST_SUCCEED
	PREPARE_SUCCEED
	PREPARE_CONFLICT
)
