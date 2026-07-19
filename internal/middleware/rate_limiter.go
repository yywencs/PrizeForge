package middleware

// Status: TODO 占位，未接入 —— RateLimiter 当前只透传请求，限流逻辑未实现。
// 接入前需先实现真正的限流（Redis 令牌桶或本地计数器），并在 server 中 Use。

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// RateLimiterConfig 是限流配置的接口。
type RateLimiterConfig interface {
	GetRateLimit() int
}

// RateLimiter 返回一个用于限流的 Gin 中间件桩（占位实现）。
// TODO: 使用 Redis 或令牌桶实现按用户维度的限流。
func RateLimiter(dcc RateLimiterConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dcc != nil && dcc.GetRateLimit() <= 0 {
			c.Next()
			return
		}
		// TODO: 实现真正的限流逻辑
		c.Next()
	}
}

// 保证 import 被使用
var _ = http.StatusTooManyRequests
