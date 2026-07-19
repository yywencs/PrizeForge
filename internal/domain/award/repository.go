package award

import "context"

type Repo interface {
	// SaveUserAwardRecord 保存结果并返回数据库中的标准记录；重复 order_id 时返回已存在记录。
	SaveUserAwardRecord(ctx context.Context, aggregate *UserAwardTaskInfo) (*UserAwardRecord, error)
	// QueryByOrderID 按 orderID 查询中奖记录（用于重试时复用已落库结果）。nil 表示未查到。
	QueryByOrderID(ctx context.Context, userID string, orderID string) (*UserAwardRecord, error)
}
