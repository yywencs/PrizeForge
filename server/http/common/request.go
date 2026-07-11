package common

import (
	"strconv"

	"github.com/gin-gonic/gin"
)

func ParseStrategyID(ctx *gin.Context) (int64, bool) {
	strategyIDStr := ctx.Query("strategy_id")
	strategyID, err := strconv.ParseInt(strategyIDStr, 10, 64)
	if err != nil {
		Error(ctx, 400, "invalid strategy_id")
		return 0, false
	}
	return strategyID, true
}
