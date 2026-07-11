package middleware

// Status: TODO 占位，未接入 —— RateLimiter 当前只透传请求，限流逻辑未实现。
// 接入前需先实现真正的限流（Redis 令牌桶或本地计数器），并在 server 中 Use。

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// RateLimiterConfig is the interface for rate limit configuration.
type RateLimiterConfig interface {
	GetRateLimit() int
}

// RateLimiter returns a Gin middleware stub for rate limiting.
// TODO: Implement per-user rate limiting using Redis or token bucket.
func RateLimiter(dcc RateLimiterConfig) gin.HandlerFunc {
	return func(c *gin.Context) {
		if dcc != nil && dcc.GetRateLimit() <= 0 {
			c.Next()
			return
		}
		// TODO: implement actual rate limiting
		c.Next()
	}
}

// Ensure import is used
var _ = http.StatusTooManyRequests
