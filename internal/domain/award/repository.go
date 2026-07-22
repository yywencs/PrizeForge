package award

import (
	"context"
	"errors"
)

var (
	ErrAwardRecordNotFound = errors.New("award record not found")
	ErrAwardStateConflict  = errors.New("award state conflict")
)

type Repo interface {
	// SaveUserAwardRecord 保存结果并返回数据库中的标准记录；重复 order_id 时返回已存在记录。
	SaveUserAwardRecord(ctx context.Context, aggregate *UserAwardTaskInfo) (*UserAwardRecord, error)
	// QueryByOrderID 按 orderID 查询中奖记录（用于重试时复用已落库结果）。nil 表示未查到。
	QueryByOrderID(ctx context.Context, userID string, orderID string) (*UserAwardRecord, error)
	// CompleteUserAward 将中奖记录从 create 幂等推进到 complete。
	CompleteUserAward(ctx context.Context, userID string, orderID string) error
}
