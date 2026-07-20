package strategy

import (
	"os"
	"testing"

	"prizeforge/pkg/logger"

	"go.uber.org/zap"
)

// TestMain 为策略领域测试安装无输出 Logger，避免测试依赖生产日志文件配置。
func TestMain(m *testing.M) {
	logger.Log = zap.NewNop()
	zap.ReplaceGlobals(logger.Log)
	os.Exit(m.Run())
}
