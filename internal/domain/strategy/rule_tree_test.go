package strategy

import (
	"context"
	"errors"
	"testing"
)

// TestRuleTreeFactoryValidatesTree 验证规则树工厂拒绝 nil 或缺少根节点的配置，
// 并能为包含有效根节点的规则树创建执行引擎。
func TestRuleTreeFactoryValidatesTree(t *testing.T) {
	factory := newRuleTreeFactory(&fakeStrategyRepository{})

	tests := []struct {
		name    string
		tree    *RuleTree
		wantErr error
	}{
		{
			name:    "nil tree",
			wantErr: ErrRuleTreeInvalid,
		},
		{
			name: "missing root node",
			tree: &RuleTree{
				TreeRootRuleNode: RuleLock,
				NodeMap:          map[RuleTreeName]*RuleTreeNode{},
			},
			wantErr: ErrRuleTreeInvalid,
		},
		{
			name: "valid root node",
			tree: &RuleTree{
				TreeRootRuleNode: RuleLock,
				NodeMap: map[RuleTreeName]*RuleTreeNode{
					RuleLock: {RuleKey: RuleLock, RuleValue: "10"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, err := factory.newDecisionTreeEngine(tt.tree)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("newDecisionTreeEngine() error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				if engine != nil {
					t.Fatalf("newDecisionTreeEngine() engine = %#v, want nil", engine)
				}
				return
			}
			if engine == nil || engine.RuleTree != tt.tree {
				t.Fatalf("newDecisionTreeEngine() engine = %#v, want tree %p", engine, tt.tree)
			}
		})
	}
}

// TestRuleLockNodeLogic 验证当前固定抽奖次数为 10 时，达到次数门槛会放行原奖品，
// 未达到门槛则交由规则树的后续兜底节点接管。
func TestRuleLockNodeLogic(t *testing.T) {
	node := newRuleLockNode()

	tests := []struct {
		name          string
		ruleValue     string
		wantCheckType RuleLogicCheckType
	}{
		{
			name:          "threshold reached",
			ruleValue:     "10",
			wantCheckType: RuleCheckAllow,
		},
		{
			name:          "threshold not reached",
			ruleValue:     "11",
			wantCheckType: RuleCheckTakeOver,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			action, err := node.logic(context.Background(), "user-1", "order-1", 100001, 101, tt.ruleValue)
			if err != nil {
				t.Fatalf("ruleLockNode.logic() error = %v, want nil", err)
			}
			if action.RuleLogicCheckType != tt.wantCheckType {
				t.Fatalf("ruleLockNode.logic() check type = %v, want %v", action.RuleLogicCheckType, tt.wantCheckType)
			}
			if action.Award.AwardID != 101 || action.Award.AwardRuleValue != tt.ruleValue {
				t.Fatalf("ruleLockNode.logic() award = %#v, want award 101 and rule %q", action.Award, tt.ruleValue)
			}
		})
	}
}

// TestRuleLuckAwardNodeLogic 验证兜底奖品规则会解析新的奖品 ID 和规则值，
// 并以 TakeOver 结果终止原奖品分支。
func TestRuleLuckAwardNodeLogic(t *testing.T) {
	node := newRuleLuckAwardNode()

	action, err := node.logic(context.Background(), "user-1", "order-1", 100001, 101, "999:兜底奖品")

	if err != nil {
		t.Fatalf("ruleLuckAwardNode.logic() error = %v, want nil", err)
	}
	if action.RuleLogicCheckType != RuleCheckTakeOver {
		t.Fatalf("ruleLuckAwardNode.logic() check type = %v, want %v", action.RuleLogicCheckType, RuleCheckTakeOver)
	}
	if action.Award.AwardID != 999 || action.Award.AwardRuleValue != "兜底奖品" {
		t.Fatalf("ruleLuckAwardNode.logic() award = %#v, want award 999 and fallback value", action.Award)
	}
}

// TestRuleStockNodeLogic 验证正式订单使用幂等库存预占且不重复发送旧队列任务，
// 并覆盖预占成功、库存不足、预占失败以及无业务订单试抽的旧扣减路径。
func TestRuleStockNodeLogic(t *testing.T) {
	reserveErr := errors.New("reserve stock")
	queueErr := errors.New("send stock task")

	tests := []struct {
		name              string
		orderID           string
		reservedAwardID   int64
		stockAvailable    bool
		stockErr          error
		queueErr          error
		wantErr           error
		wantCheckType     RuleLogicCheckType
		wantAwardID       int64
		wantReserved      bool
		wantReserveCalls  int
		wantSubtractCalls int
		wantQueueCalls    int
	}{
		{
			name:             "formal order reserves stock",
			orderID:          "order-1",
			reservedAwardID:  202,
			stockAvailable:   true,
			wantCheckType:    RuleCheckAllow,
			wantAwardID:      202,
			wantReserved:     true,
			wantReserveCalls: 1,
		},
		{
			name:             "formal order stock exhausted",
			orderID:          "order-1",
			reservedAwardID:  101,
			wantCheckType:    RuleCheckTakeOver,
			wantAwardID:      101,
			wantReserveCalls: 1,
		},
		{
			name:             "formal order reservation failure",
			orderID:          "order-1",
			stockErr:         reserveErr,
			wantErr:          reserveErr,
			wantReserveCalls: 1,
		},
		{
			name:              "trial draw subtracts and queues stock",
			stockAvailable:    true,
			wantCheckType:     RuleCheckAllow,
			wantAwardID:       101,
			wantSubtractCalls: 1,
			wantQueueCalls:    1,
		},
		{
			name:              "trial draw queue failure",
			stockAvailable:    true,
			queueErr:          queueErr,
			wantErr:           queueErr,
			wantSubtractCalls: 1,
			wantQueueCalls:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reserveCalls := 0
			subtractCalls := 0
			queueCalls := 0
			repo := &fakeStrategyRepository{
				reserveAwardStockFn: func(_ context.Context, userID string, orderID string, strategyID int64, awardID int64) (int64, bool, error) {
					reserveCalls++
					if userID != "user-1" || orderID != tt.orderID || strategyID != 100001 || awardID != 101 {
						t.Fatalf("ReserveAwardStock() args = (%q, %q, %d, %d)", userID, orderID, strategyID, awardID)
					}
					return tt.reservedAwardID, tt.stockAvailable, tt.stockErr
				},
				subtractionAwardStockFn: func(_ context.Context, strategyID int64, awardID int64) (bool, error) {
					subtractCalls++
					if strategyID != 100001 || awardID != 101 {
						t.Fatalf("SubtractionAwardStock() args = (%d, %d)", strategyID, awardID)
					}
					return tt.stockAvailable, tt.stockErr
				},
				awardStockConsumeSendQueueFn: func(_ context.Context, userID string, orderID string, strategyID int64, awardID int64) error {
					queueCalls++
					if userID != "user-1" || orderID != "" || strategyID != 100001 || awardID != 101 {
						t.Fatalf("AwardStockConsumeSendQueue() args = (%q, %q, %d, %d)", userID, orderID, strategyID, awardID)
					}
					return tt.queueErr
				},
			}
			node := newRuleStockNode(repo)

			action, err := node.logic(context.Background(), "user-1", tt.orderID, 100001, 101, "stock-rule")

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ruleStockNode.logic() error = %v, want %v", err, tt.wantErr)
			}
			if reserveCalls != tt.wantReserveCalls || subtractCalls != tt.wantSubtractCalls || queueCalls != tt.wantQueueCalls {
				t.Fatalf("repository calls = reserve:%d subtract:%d queue:%d, want reserve:%d subtract:%d queue:%d", reserveCalls, subtractCalls, queueCalls, tt.wantReserveCalls, tt.wantSubtractCalls, tt.wantQueueCalls)
			}
			if tt.wantErr != nil {
				if action != nil {
					t.Fatalf("ruleStockNode.logic() action = %#v, want nil", action)
				}
				return
			}
			if action.RuleLogicCheckType != tt.wantCheckType || action.Award.AwardID != tt.wantAwardID || action.Award.StockReserved != tt.wantReserved {
				t.Fatalf("ruleStockNode.logic() action = %#v, want check=%v award=%d reserved=%v", action, tt.wantCheckType, tt.wantAwardID, tt.wantReserved)
			}
		})
	}
}

// TestRuleTreeEngineStockSuccessTerminates 验证库存预占成功后，即使规则树只配置了
// 库存不足的兜底分支，也会正常结束并返回已预占的奖品。
func TestRuleTreeEngineStockSuccessTerminates(t *testing.T) {
	repo := &fakeStrategyRepository{
		reserveAwardStockFn: func(_ context.Context, userID string, orderID string, strategyID int64, awardID int64) (int64, bool, error) {
			if userID != "user-1" || orderID != "order-1" || strategyID != 100001 || awardID != 101 {
				t.Fatalf("ReserveAwardStock() args = (%q, %q, %d, %d)", userID, orderID, strategyID, awardID)
			}
			return 101, true, nil
		},
	}
	engine := newTestRuleTreeEngine(t, repo, &RuleTree{
		TreeRootRuleNode: RuleLock,
		NodeMap: map[RuleTreeName]*RuleTreeNode{
			RuleLock: {
				RuleKey:   RuleLock,
				RuleValue: "1",
				TreeNodeLine: []*RuleTreeNodeLine{
					{
						RuleNodeFrom:   string(RuleLock),
						RuleNodeTo:     string(RuleStock),
						RuleLimitType:  EQUAL,
						RuleLimitValue: RuleCheckAllow,
					},
					{
						RuleNodeFrom:   string(RuleLock),
						RuleNodeTo:     string(RuleLuckAward),
						RuleLimitType:  EQUAL,
						RuleLimitValue: RuleCheckTakeOver,
					},
				},
			},
			RuleStock: {
				RuleKey: RuleStock,
				TreeNodeLine: []*RuleTreeNodeLine{
					{
						RuleNodeFrom:   string(RuleStock),
						RuleNodeTo:     string(RuleLuckAward),
						RuleLimitType:  EQUAL,
						RuleLimitValue: RuleCheckTakeOver,
					},
				},
			},
			RuleLuckAward: {
				RuleKey:   RuleLuckAward,
				RuleValue: "999:兜底奖品",
			},
		},
	})

	award, err := engine.process(context.Background(), "user-1", "order-1", 100001, 101)

	if err != nil {
		t.Fatalf("process() error = %v, want nil", err)
	}
	if award.AwardID != 101 || !award.StockReserved {
		t.Fatalf("process() award = %#v, want reserved award 101", award)
	}
}

// TestRuleTreeEngineStockExhaustedUsesFallback 验证库存不足时会按照 TAKE_OVER
// 分支进入兜底节点，并返回兜底奖品。
func TestRuleTreeEngineStockExhaustedUsesFallback(t *testing.T) {
	repo := &fakeStrategyRepository{
		reserveAwardStockFn: func(context.Context, string, string, int64, int64) (int64, bool, error) {
			return 101, false, nil
		},
	}
	engine := newTestRuleTreeEngine(t, repo, &RuleTree{
		TreeRootRuleNode: RuleStock,
		NodeMap: map[RuleTreeName]*RuleTreeNode{
			RuleStock: {
				RuleKey: RuleStock,
				TreeNodeLine: []*RuleTreeNodeLine{
					{
						RuleNodeFrom:   string(RuleStock),
						RuleNodeTo:     string(RuleLuckAward),
						RuleLimitType:  EQUAL,
						RuleLimitValue: RuleCheckTakeOver,
					},
				},
			},
			RuleLuckAward: {
				RuleKey:   RuleLuckAward,
				RuleValue: "999:兜底奖品",
			},
		},
	})

	award, err := engine.process(context.Background(), "user-1", "order-1", 100001, 101)

	if err != nil {
		t.Fatalf("process() error = %v, want nil", err)
	}
	if award.AwardID != 999 || award.StockReserved {
		t.Fatalf("process() award = %#v, want unreserved fallback award 999", award)
	}
}

// TestRuleTreeEngineRejectsUnmatchedBranch 验证非库存节点存在连线但没有任何
// 条件与执行结果匹配时返回规则树错误，而不是 panic 或静默结束。
func TestRuleTreeEngineRejectsUnmatchedBranch(t *testing.T) {
	engine := newTestRuleTreeEngine(t, &fakeStrategyRepository{}, &RuleTree{
		TreeRootRuleNode: RuleLock,
		NodeMap: map[RuleTreeName]*RuleTreeNode{
			RuleLock: {
				RuleKey:   RuleLock,
				RuleValue: "10",
				TreeNodeLine: []*RuleTreeNodeLine{
					{
						RuleNodeFrom:   string(RuleLock),
						RuleNodeTo:     string(RuleLuckAward),
						RuleLimitType:  EQUAL,
						RuleLimitValue: RuleCheckTakeOver,
					},
				},
			},
			RuleLuckAward: {
				RuleKey:   RuleLuckAward,
				RuleValue: "999:兜底奖品",
			},
		},
	})

	award, err := engine.process(context.Background(), "user-1", "order-1", 100001, 101)

	if !errors.Is(err, ErrRuleTreeInvalid) {
		t.Fatalf("process() error = %v, want %v", err, ErrRuleTreeInvalid)
	}
	if award != nil {
		t.Fatalf("process() award = %#v, want nil", award)
	}
}

func newTestRuleTreeEngine(t *testing.T, repo Repo, ruleTree *RuleTree) *ruleTreeEngine {
	t.Helper()
	engine, err := newRuleTreeFactory(repo).newDecisionTreeEngine(ruleTree)
	if err != nil {
		t.Fatalf("newDecisionTreeEngine() error = %v, want nil", err)
	}
	return engine
}
