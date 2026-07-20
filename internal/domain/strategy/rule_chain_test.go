package strategy

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// TestLogicFactoryOpenLogicChain 验证责任链会保持已知规则的配置顺序、
// 跳过中间未知规则、自动追加默认规则，并在首节点未知时安全降级到默认规则。
func TestLogicFactoryOpenLogicChain(t *testing.T) {
	tests := []struct {
		name       string
		ruleModels string
		want       []RuleChainName
	}{
		{
			name: "empty rules use default",
			want: []RuleChainName{RuleDefault},
		},
		{
			name:       "known rules keep order",
			ruleModels: "rule_blacklist,rule_weight",
			want:       []RuleChainName{RuleBlacklist, RuleWeight, RuleDefault},
		},
		{
			name:       "unknown middle rule is skipped",
			ruleModels: "rule_blacklist,unknown,rule_weight",
			want:       []RuleChainName{RuleBlacklist, RuleWeight, RuleDefault},
		},
		{
			name:       "unknown first rule falls back to default",
			ruleModels: "unknown,rule_weight",
			want:       []RuleChainName{RuleDefault},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &fakeStrategyRepository{
				queryStrategyEntityFn: func(_ context.Context, strategyID int64) (*Strategy, error) {
					if strategyID != 100001 {
						t.Fatalf("QueryStrategyEntityByStrategyId() strategyID = %d, want 100001", strategyID)
					}
					return &Strategy{StrategyID: strategyID, RuleModels: tt.ruleModels}, nil
				},
			}
			factory := newLogicFactory(repo, newArmoryDispatch(repo))

			chain, err := factory.openLogicChain(context.Background(), 100001)
			if err != nil {
				t.Fatalf("openLogicChain() error = %v, want nil", err)
			}

			var got []RuleChainName
			for node := chain; node != nil; node = node.next() {
				got = append(got, node.ruleModel())
				if len(got) > len(factory.logicChainGroup)+1 {
					t.Fatal("openLogicChain() produced a cycle")
				}
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("openLogicChain() models = %#v, want %#v", got, tt.want)
			}
		})
	}
}

// TestLogicFactoryOpenLogicChainPropagatesRepositoryError 验证查询策略配置失败时，
// 责任链工厂不会构造降级链，而是把仓储错误原样返回给调用方。
func TestLogicFactoryOpenLogicChainPropagatesRepositoryError(t *testing.T) {
	repositoryErr := errors.New("query strategy")
	repo := &fakeStrategyRepository{
		queryStrategyEntityFn: func(context.Context, int64) (*Strategy, error) {
			return nil, repositoryErr
		},
	}
	factory := newLogicFactory(repo, newArmoryDispatch(repo))

	chain, err := factory.openLogicChain(context.Background(), 100001)

	if !errors.Is(err, repositoryErr) {
		t.Fatalf("openLogicChain() error = %v, want %v", err, repositoryErr)
	}
	if chain != nil {
		t.Fatalf("openLogicChain() chain = %#v, want nil", chain)
	}
}

// TestRuleWeightLogicGetAnalyticalValue 验证权重责任链能够解析多个权重档位，
// 忽略多余空白，并拒绝缺少冒号或使用非数字阈值的配置。
func TestRuleWeightLogicGetAnalyticalValue(t *testing.T) {
	logic := &ruleWeightLogic{}

	tests := []struct {
		name      string
		ruleValue string
		want      map[int]string
		wantError bool
	}{
		{
			name:      "empty value",
			ruleValue: "",
			want:      map[int]string{},
		},
		{
			name:      "multiple thresholds",
			ruleValue: "100:101,102 500:103",
			want: map[int]string{
				100: "101,102",
				500: "103",
			},
		},
		{
			name:      "extra spaces",
			ruleValue: " 100:101  500:103 ",
			want: map[int]string{
				100: "101",
				500: "103",
			},
		},
		{
			name:      "missing colon",
			ruleValue: "100",
			wantError: true,
		},
		{
			name:      "non-numeric threshold",
			ruleValue: "invalid:101",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := logic.getAnalyticalValue(tt.ruleValue)
			if tt.wantError {
				if err == nil {
					t.Fatal("getAnalyticalValue() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("getAnalyticalValue() error = %v, want nil", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("getAnalyticalValue() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
