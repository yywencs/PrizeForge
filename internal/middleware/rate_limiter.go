package middleware

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
