package award

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeAwardRepository struct {
	saveUserAwardRecordFn func(context.Context, *UserAwardTaskInfo) (*UserAwardRecord, error)
	queryByOrderIDFn      func(context.Context, string, string) (*UserAwardRecord, error)
	completeUserAwardFn   func(context.Context, string, string) error
}

func (f *fakeAwardRepository) SaveUserAwardRecord(ctx context.Context, aggregate *UserAwardTaskInfo) (*UserAwardRecord, error) {
	if f.saveUserAwardRecordFn == nil {
		panic("unexpected SaveUserAwardRecord call")
	}
	return f.saveUserAwardRecordFn(ctx, aggregate)
}

func (f *fakeAwardRepository) QueryByOrderID(ctx context.Context, userID string, orderID string) (*UserAwardRecord, error) {
	if f.queryByOrderIDFn == nil {
		panic("unexpected QueryByOrderID call")
	}
	return f.queryByOrderIDFn(ctx, userID, orderID)
}

func (f *fakeAwardRepository) CompleteUserAward(ctx context.Context, userID string, orderID string) error {
	if f.completeUserAwardFn == nil {
		panic("unexpected CompleteUserAward call")
	}
	return f.completeUserAwardFn(ctx, userID, orderID)
}

// TestAwardUsecaseSaveUserAwardRecordBuildsTask 验证保存中奖记录时会同时构造一条
// send_award 任务，并使用 userID+orderID 作为跨分片幂等消息 ID，完整传递发奖消息字段。
func TestAwardUsecaseSaveUserAwardRecordBuildsTask(t *testing.T) {
	record := &UserAwardRecord{
		UserID:        "user-1",
		ActivityID:    100301,
		StrategyID:    100001,
		OrderID:       "000000000001",
		AwardID:       101,
		AwardTitle:    "一等奖",
		AwardTime:     time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC),
		AwardState:    AwardStateCreate,
		StockReserved: true,
		DrawOwner:     "owner-1",
	}
	canonical := &UserAwardRecord{
		UserID:     record.UserID,
		OrderID:    record.OrderID,
		AwardID:    record.AwardID,
		AwardTitle: record.AwardTitle,
	}

	var captured *UserAwardTaskInfo
	repo := &fakeAwardRepository{
		saveUserAwardRecordFn: func(_ context.Context, aggregate *UserAwardTaskInfo) (*UserAwardRecord, error) {
			captured = aggregate
			return canonical, nil
		},
	}
	usecase := NewAwardUsecase(repo)

	got, err := usecase.SaveUserAwardRecord(context.Background(), record)
	if err != nil {
		t.Fatalf("SaveUserAwardRecord() error = %v, want nil", err)
	}
	if got != canonical {
		t.Fatalf("SaveUserAwardRecord() result = %p, want repository canonical record %p", got, canonical)
	}
	if captured == nil {
		t.Fatal("SaveUserAwardRecord() repository aggregate = nil")
	}
	if captured.UserAwardRecord != record {
		t.Fatal("SaveUserAwardRecord() did not pass the original award record to the repository")
	}
	if captured.Task == nil {
		t.Fatal("SaveUserAwardRecord() task = nil")
	}

	task := captured.Task
	if task.UserID != record.UserID {
		t.Fatalf("task.UserID = %q, want %q", task.UserID, record.UserID)
	}
	if task.Topic != SendAwardTopic {
		t.Fatalf("task.Topic = %q, want %q", task.Topic, SendAwardTopic)
	}
	if task.MessageID != "user-1:000000000001" {
		t.Fatalf("task.MessageID = %q, want %q", task.MessageID, "user-1:000000000001")
	}
	if task.State != TaskStateCreate {
		t.Fatalf("task.State = %q, want %q", task.State, TaskStateCreate)
	}
	if task.Message.UserID != record.UserID || task.Message.OrderID != record.OrderID ||
		task.Message.AwardID != record.AwardID || task.Message.AwardTitle != record.AwardTitle {
		t.Fatalf("task.Message = %#v, want user=%q order=%q award=%d title=%q", task.Message, record.UserID, record.OrderID, record.AwardID, record.AwardTitle)
	}
}

// TestAwardUsecaseCompleteUserAwardDelegatesToRepository 验证发奖消费者确认完成时，
// AwardUsecase 会原样传递用户与订单幂等键，并保留仓储返回的错误。
func TestAwardUsecaseCompleteUserAwardDelegatesToRepository(t *testing.T) {
	repositoryErr := errors.New("complete award")
	repo := &fakeAwardRepository{
		completeUserAwardFn: func(_ context.Context, userID string, orderID string) error {
			if userID != "user-1" || orderID != "000000000001" {
				t.Fatalf("CompleteUserAward() args = (%q, %q), want expected identity", userID, orderID)
			}
			return repositoryErr
		},
	}

	err := NewAwardUsecase(repo).CompleteUserAward(context.Background(), "user-1", "000000000001")
	if !errors.Is(err, repositoryErr) {
		t.Fatalf("CompleteUserAward() error = %v, want %v", err, repositoryErr)
	}
}

// TestAwardUsecaseSaveUserAwardRecordPropagatesRepositoryError 验证仓储保存中奖记录失败时，
// AwardUsecase 不会吞掉或替换错误，而是将原始错误返回给上层抽奖编排。
func TestAwardUsecaseSaveUserAwardRecordPropagatesRepositoryError(t *testing.T) {
	repositoryErr := errors.New("save award record")
	repo := &fakeAwardRepository{
		saveUserAwardRecordFn: func(context.Context, *UserAwardTaskInfo) (*UserAwardRecord, error) {
			return nil, repositoryErr
		},
	}
	usecase := NewAwardUsecase(repo)

	got, err := usecase.SaveUserAwardRecord(context.Background(), &UserAwardRecord{
		UserID:  "user-1",
		OrderID: "000000000001",
	})

	if !errors.Is(err, repositoryErr) {
		t.Fatalf("SaveUserAwardRecord() error = %v, want %v", err, repositoryErr)
	}
	if got != nil {
		t.Fatalf("SaveUserAwardRecord() result = %#v, want nil", got)
	}
}

// TestAwardUsecaseQueryByOrderIDDelegatesToRepository 验证订单幂等查询会把用户 ID 和订单 ID
// 原样传给仓储，并将已存在记录、未命中结果或查询错误原样返回。
func TestAwardUsecaseQueryByOrderIDDelegatesToRepository(t *testing.T) {
	repositoryErr := errors.New("query award record")
	existing := &UserAwardRecord{
		UserID:  "user-1",
		OrderID: "000000000001",
		AwardID: 101,
	}

	tests := []struct {
		name    string
		record  *UserAwardRecord
		repoErr error
	}{
		{
			name:   "existing record",
			record: existing,
		},
		{
			name: "record not found",
		},
		{
			name:    "repository failure",
			repoErr: repositoryErr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeAwardRepository{
				queryByOrderIDFn: func(_ context.Context, userID string, orderID string) (*UserAwardRecord, error) {
					if userID != "user-1" || orderID != "000000000001" {
						t.Fatalf("QueryByOrderID() args = (%q, %q), want (%q, %q)", userID, orderID, "user-1", "000000000001")
					}
					return tt.record, tt.repoErr
				},
			}
			usecase := NewAwardUsecase(repo)

			got, err := usecase.QueryByOrderID(context.Background(), "user-1", "000000000001")

			if !errors.Is(err, tt.repoErr) {
				t.Fatalf("QueryByOrderID() error = %v, want %v", err, tt.repoErr)
			}
			if got != tt.record {
				t.Fatalf("QueryByOrderID() result = %p, want %p", got, tt.record)
			}
		})
	}
}
