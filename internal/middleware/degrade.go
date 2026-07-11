package middleware

// Status: 已实现，未接入 —— Degrade 中间件当前没有挂进任何 HTTP server。
// 接入前需提供一个实现 DegradeConfig 的配置来源（例如 viper 降级开关），并在 server
// 的中间件链里 Use(middleware.Degrade(degradeConfig))。

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// DegradeConfig is the interface for checking the degrade switch.
type DegradeConfig interface {
	IsDegraded() bool
}

// Degrade returns a Gin middleware that returns 503 when the service is degraded.
func Degrade(dcc DegradeConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dcc != nil && dcc.IsDegraded() {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{
				"code": 503,
				"info": "service degraded",
				"data": nil,
			})
			return
		}
		c.Next()
	}
}
