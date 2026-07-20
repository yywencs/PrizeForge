package strategy

import (
	"errors"
	"reflect"
	"testing"
)

// TestStrategyRuleModelSelection 验证策略规则字符串能够按配置顺序解析，
// 并能正确识别是否包含黑名单规则和权重规则。
func TestStrategyRuleModelSelection(t *testing.T) {
	tests := []struct {
		name          string
		ruleModels    string
		wantModels    []RuleChainName
		wantBlacklist string
		wantWeight    string
	}{
		{
			name: "empty rules",
		},
		{
			name:          "blacklist only",
			ruleModels:    "rule_blacklist",
			wantModels:    []RuleChainName{RuleBlacklist},
			wantBlacklist: "rule_blacklist",
		},
		{
			name:          "blacklist and weight",
			ruleModels:    "rule_blacklist,rule_weight",
			wantModels:    []RuleChainName{RuleBlacklist, RuleWeight},
			wantBlacklist: "rule_blacklist",
			wantWeight:    "rule_weight",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := &Strategy{RuleModels: tt.ruleModels}

			if got := strategy.GetRuleModels(); !reflect.DeepEqual(got, tt.wantModels) {
				t.Fatalf("GetRuleModels() = %#v, want %#v", got, tt.wantModels)
			}
			if got := strategy.GetRuleBlackList(); got != tt.wantBlacklist {
				t.Fatalf("GetRuleBlackList() = %q, want %q", got, tt.wantBlacklist)
			}
			if got := strategy.GetRuleWeight(); got != tt.wantWeight {
				t.Fatalf("GetRuleWeight() = %q, want %q", got, tt.wantWeight)
			}
		})
	}
}

// TestStrategyRuleGetRuleWeightValues 验证权重规则能解析为阈值到奖品列表的映射，
// 非权重规则会被忽略，缺少分隔符或包含非法奖品 ID 时返回结构化错误而不是 panic。
func TestStrategyRuleGetRuleWeightValues(t *testing.T) {
	tests := []struct {
		name      string
		rule      StrategyRule
		want      map[string][]int64
		wantNil   bool
		wantError error
	}{
		{
			name: "non-weight rule",
			rule: StrategyRule{
				RuleModel: "rule_blacklist",
				RuleValue: "101:user-1",
			},
			wantNil: true,
		},
		{
			name: "valid weight groups",
			rule: StrategyRule{
				RuleModel: "rule_weight",
				RuleValue: "100:101,102 500:103",
			},
			want: map[string][]int64{
				"100": {101, 102},
				"500": {103},
			},
		},
		{
			name: "missing colon",
			rule: StrategyRule{
				RuleModel: "rule_weight",
				RuleValue: "100",
			},
			wantError: ErrRuleWeightValueInvalidFormat,
		},
		{
			name: "empty award list",
			rule: StrategyRule{
				RuleModel: "rule_weight",
				RuleValue: "100:",
			},
			wantError: ErrRuleWeightValueInvalidFormat,
		},
		{
			name: "non-numeric award ID",
			rule: StrategyRule{
				RuleModel: "rule_weight",
				RuleValue: "100:invalid",
			},
			wantError: ErrRuleWeightValueInvalidFormat,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.rule.GetRuleWeightValues()

			if !errors.Is(err, tt.wantError) {
				t.Fatalf("GetRuleWeightValues() error = %v, want %v", err, tt.wantError)
			}
			if tt.wantError != nil {
				return
			}
			if tt.wantNil {
				if got != nil {
					t.Fatalf("GetRuleWeightValues() = %#v, want nil", got)
				}
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("GetRuleWeightValues() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
