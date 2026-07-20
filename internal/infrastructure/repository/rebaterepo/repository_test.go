package rebaterepo

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"prizeforge/internal/domain/rebate"

	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type fakeDatabaseRouter struct {
	db          *gorm.DB
	tableSuffix string
	wantKey     string
	t           *testing.T
}

func (f *fakeDatabaseRouter) DBStrategy(shardKey string) (*gorm.DB, string) {
	f.t.Helper()
	if shardKey != f.wantKey {
		f.t.Fatalf("DBStrategy() shardKey = %q, want %q", shardKey, f.wantKey)
	}
	return f.db, f.tableSuffix
}

func newMockGormDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() {
		_ = sqlDB.Close()
	})

	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      sqlDB,
		SkipInitializeWithVersion: true,
	}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("gorm.Open() error = %v", err)
	}
	return db, mock
}

// TestRebateRepositoryQueryDailyBehaviorRebateConfig 验证返利配置查询只读取开启状态的
// 指定行为配置，并把数据库记录完整转换为领域实体。
func TestRebateRepositoryQueryDailyBehaviorRebateConfig(t *testing.T) {
	db, mock := newMockGormDB(t)
	createdAt := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT * FROM `daily_behavior_rebate` WHERE behavior_type = ? AND state = ?",
	)).WithArgs(rebate.Sign, "open").WillReturnRows(sqlmock.NewRows([]string{
		"id", "behavior_type", "rebate_desc", "rebate_type", "rebate_config", "state", "create_time", "update_time",
	}).AddRow(1, string(rebate.Sign), "签到赠送抽奖次数", string(rebate.Sku), "9011", "open", createdAt, createdAt))
	repository := &rebateRepository{db: db}

	configs, err := repository.QueryDailyBehaviorRebateConfig(context.Background(), rebate.Sign)

	if err != nil {
		t.Fatalf("QueryDailyBehaviorRebateConfig() error = %v, want nil", err)
	}
	if len(configs) != 1 {
		t.Fatalf("QueryDailyBehaviorRebateConfig() count = %d, want 1", len(configs))
	}
	config := configs[0]
	if config.BehaviorType != string(rebate.Sign) || config.RebateDesc != "签到赠送抽奖次数" || config.RebateType != string(rebate.Sku) || config.RebateConfig != "9011" {
		t.Fatalf("QueryDailyBehaviorRebateConfig() config = %#v, want complete mapped entity", config)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("database expectations were not met: %v", err)
	}
}

// TestRebateRepositoryQueryDailyBehaviorRebateConfigReturnsDatabaseError 验证配置查询失败时，
// 仓储不会返回部分结果或吞掉错误，而是把数据库错误原样交给领域层。
func TestRebateRepositoryQueryDailyBehaviorRebateConfigReturnsDatabaseError(t *testing.T) {
	db, mock := newMockGormDB(t)
	databaseErr := errors.New("query daily rebate")
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT * FROM `daily_behavior_rebate` WHERE behavior_type = ? AND state = ?",
	)).WithArgs(rebate.Sign, "open").WillReturnError(databaseErr)
	repository := &rebateRepository{db: db}

	configs, err := repository.QueryDailyBehaviorRebateConfig(context.Background(), rebate.Sign)

	if !errors.Is(err, databaseErr) {
		t.Fatalf("QueryDailyBehaviorRebateConfig() error = %v, want %v", err, databaseErr)
	}
	if configs != nil {
		t.Fatalf("QueryDailyBehaviorRebateConfig() configs = %#v, want nil", configs)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("database expectations were not met: %v", err)
	}
}

// TestRebateRepositoryQueryUserRebateOrderUsesOutBusinessNo 验证签到查询会路由到正确分表，
// 使用独立的 out_business_no 列精确匹配日期，并把数据库订单完整转换为领域实体。
func TestRebateRepositoryQueryUserRebateOrderUsesOutBusinessNo(t *testing.T) {
	db, mock := newMockGormDB(t)
	createdAt := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT * FROM `user_behavior_rebate_order_007` WHERE user_id = ? AND out_business_no = ?",
	)).WithArgs("user-1", "20260720").WillReturnRows(sqlmock.NewRows([]string{
		"id", "user_id", "order_id", "behavior_type", "out_business_no", "rebate_desc", "rebate_type", "rebate_config", "biz_id", "create_time", "update_time",
	}).AddRow(1, "user-1", "000000000001", string(rebate.Sign), "20260720", "签到赠送抽奖次数", string(rebate.Sku), "9011", "user-1_sku_20260720", createdAt, createdAt))
	repository := &rebateRepository{
		routerDB: &fakeDatabaseRouter{
			db:          db,
			tableSuffix: "007",
			wantKey:     "user-1",
			t:           t,
		},
	}

	orders, err := repository.QueryUserRebateOrder(context.Background(), "user-1", "20260720")

	if err != nil {
		t.Fatalf("QueryUserRebateOrder() error = %v, want nil", err)
	}
	if len(orders) != 1 {
		t.Fatalf("QueryUserRebateOrder() count = %d, want 1", len(orders))
	}
	order := orders[0]
	if order.UserID != "user-1" || order.OrderID != "000000000001" || order.BehaviorType != string(rebate.Sign) || order.OutBusinessNo != "20260720" {
		t.Fatalf("QueryUserRebateOrder() identity fields = %#v, want complete mapped order", order)
	}
	if order.RebateDesc != "签到赠送抽奖次数" || order.RebateType != string(rebate.Sku) || order.RebateConfig != "9011" || order.BizID != "user-1_sku_20260720" {
		t.Fatalf("QueryUserRebateOrder() rebate fields = %#v, want complete mapped order", order)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("database expectations were not met: %v", err)
	}
}

// TestRebateRepositoryQueryUserRebateOrderHandlesFailures 验证分片不存在时返回明确领域错误，
// 数据库查询失败时返回原始错误，避免 nil 数据库崩溃或把失败误判成尚未签到。
func TestRebateRepositoryQueryUserRebateOrderHandlesFailures(t *testing.T) {
	databaseErr := errors.New("query user rebate order")
	tests := []struct {
		name      string
		routerDB  *gorm.DB
		prepare   func(sqlmock.Sqlmock)
		wantErr   error
		checkMock bool
	}{
		{
			name:    "route not found",
			wantErr: rebate.ErrDBRouterNotFound,
		},
		{
			name:      "database failure",
			wantErr:   databaseErr,
			checkMock: true,
			prepare: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(regexp.QuoteMeta(
					"SELECT * FROM `user_behavior_rebate_order_007` WHERE user_id = ? AND out_business_no = ?",
				)).WithArgs("user-1", "20260720").WillReturnError(databaseErr)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mock sqlmock.Sqlmock
			if tt.checkMock {
				tt.routerDB, mock = newMockGormDB(t)
				tt.prepare(mock)
			}
			repository := &rebateRepository{
				routerDB: &fakeDatabaseRouter{
					db:          tt.routerDB,
					tableSuffix: "007",
					wantKey:     "user-1",
					t:           t,
				},
			}

			orders, err := repository.QueryUserRebateOrder(context.Background(), "user-1", "20260720")

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("QueryUserRebateOrder() error = %v, want %v", err, tt.wantErr)
			}
			if orders != nil {
				t.Fatalf("QueryUserRebateOrder() orders = %#v, want nil", orders)
			}
			if tt.checkMock {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatalf("database expectations were not met: %v", err)
				}
			}
		})
	}
}
