//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"prizeforge/internal/domain/award"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/internal/infrastructure/repository/taskrepo"
	"prizeforge/pkg/xrand"

	"gorm.io/gorm"
)

// TestTaskRepositoryScansExpiredFailuresThenCreatedTasks 验证真实 task 表的扫描契约：
// 只重试超过 6 分钟的 fail 任务，随后立即处理 create 任务，并排除近期失败、已完成和超过上限的任务。
func TestTaskRepositoryScansExpiredFailuresThenCreatedTasks(t *testing.T) {
	db := integrationDBRouter.GetDB(1)
	if db == nil {
		t.Fatal("GetDB(1) = nil")
	}
	userID := "it-task-scan-" + xrand.RandomNumeric(12)
	t.Cleanup(func() {
		deleteIntegrationRows(t, db, "task", "user_id", userID)
	})

	now := time.Now().Truncate(time.Second)
	tasks := []po.Task{
		newIntegrationTask(userID, "fail-expired-oldest", "fail", now.Add(-20*time.Minute)),
		newIntegrationTask(userID, "fail-expired-middle", "fail", now.Add(-12*time.Minute)),
		newIntegrationTask(userID, "fail-recent", "fail", now.Add(-time.Minute)),
	}
	wantMessageIDs := []string{"fail-expired-oldest", "fail-expired-middle"}

	for index := 0; index < 9; index++ {
		messageID := fmt.Sprintf("create-immediate-%02d", index)
		updateTime := now.Add(time.Duration(-300+index) * time.Second)
		tasks = append(tasks, newIntegrationTask(userID, messageID, "create", updateTime))
		if index < 8 {
			wantMessageIDs = append(wantMessageIDs, messageID)
		}
	}
	tasks = append(tasks,
		newIntegrationTask(userID, "completed-old", "completed", now.Add(-time.Hour)),
	)
	if err := db.Create(&tasks).Error; err != nil {
		t.Fatalf("prepare task scan fixtures: %v", err)
	}

	repository := taskrepo.NewTaskRepository(integrationDBRouter, nil)
	got, err := repository.QueryNoSendMessageTaskList(context.Background(), 1)
	if err != nil {
		t.Fatalf("QueryNoSendMessageTaskList() error = %v, want nil", err)
	}
	if len(got) != 10 {
		t.Fatalf("QueryNoSendMessageTaskList() count = %d, want 10", len(got))
	}
	for index, task := range got {
		if task.MessageID != wantMessageIDs[index] {
			t.Fatalf("task[%d].MessageID = %q, want %q", index, task.MessageID, wantMessageIDs[index])
		}
	}
}

// TestTaskRepositoryUpdatesStateInUserShard 验证 completed 和 fail 状态更新会根据 userID
// 定位到正确分库；即使两个分库存在相同 message_id，也只更新目标用户所在库的记录。
func TestTaskRepositoryUpdatesStateInUserShard(t *testing.T) {
	seed := xrand.RandomNumeric(12)
	userDB01 := findIntegrationUserForDB(t, 1, "t1"+seed)
	userDB02 := findIntegrationUserForDB(t, 2, "t2"+seed)
	messageID := "it-task-state-" + seed
	now := time.Now().Truncate(time.Second)

	db01 := integrationDBRouter.GetDB(1)
	db02 := integrationDBRouter.GetDB(2)
	t.Cleanup(func() {
		deleteIntegrationRows(t, db01, "task", "user_id", userDB01)
		deleteIntegrationRows(t, db02, "task", "user_id", userDB02)
	})
	taskDB01 := newIntegrationTask(userDB01, messageID, "create", now)
	taskDB02 := newIntegrationTask(userDB02, messageID, "create", now)
	if err := db01.Create(&taskDB01).Error; err != nil {
		t.Fatalf("prepare database 01 task: %v", err)
	}
	if err := db02.Create(&taskDB02).Error; err != nil {
		t.Fatalf("prepare database 02 task: %v", err)
	}

	repository := taskrepo.NewTaskRepository(integrationDBRouter, nil)
	if err := repository.UpdateTaskSendMessageCompleted(context.Background(), userDB01, messageID); err != nil {
		t.Fatalf("UpdateTaskSendMessageCompleted() error = %v, want nil", err)
	}
	if err := repository.UpdateTaskSendMessageFail(context.Background(), userDB02, messageID); err != nil {
		t.Fatalf("UpdateTaskSendMessageFail() error = %v, want nil", err)
	}

	assertIntegrationTaskState(t, db01, userDB01, messageID, "completed")
	assertIntegrationTaskState(t, db02, userDB02, messageID, "fail")
}

func newIntegrationTask(userID, messageID, state string, updateTime time.Time) po.Task {
	return po.Task{
		UserID:     userID,
		Topic:      award.SendAwardTopic,
		MessageID:  messageID,
		Message:    "{}",
		State:      state,
		CreateTime: updateTime,
		UpdateTime: updateTime,
	}
}

func findIntegrationUserForDB(t *testing.T, dbIndex int, seed string) string {
	t.Helper()
	targetDB := integrationDBRouter.GetDB(dbIndex)
	if targetDB == nil {
		t.Fatalf("GetDB(%d) = nil", dbIndex)
	}
	for candidate := 0; candidate < 10000; candidate++ {
		userID := fmt.Sprintf("it-%s-%04d", seed, candidate)
		db, _ := integrationDBRouter.DBStrategy(userID)
		if db == targetDB {
			return userID
		}
	}
	t.Fatalf("cannot find integration user for database %d", dbIndex)
	return ""
}

func assertIntegrationTaskState(t *testing.T, db *gorm.DB, userID, messageID, wantState string) {
	t.Helper()
	var stored po.Task
	if err := db.Where("user_id = ? AND message_id = ?", userID, messageID).First(&stored).Error; err != nil {
		t.Fatalf("query task state for user %s: %v", userID, err)
	}
	if stored.State != wantState {
		t.Fatalf("task state for user %s = %q, want %q", userID, stored.State, wantState)
	}
}
