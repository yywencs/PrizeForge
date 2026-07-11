package middleware

import (
	"prizeforge/internal/metrics"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// PrometheusMetrics returns a Gin middleware that records HTTP request metrics.
func PrometheusMetrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		// Process request
		c.Next()

		// Record metrics after response
		duration := time.Since(start)
		method := c.Request.Method
		path := c.FullPath()
		if path == "" {
			path = c.Request.URL.Path
		}
		code := strconv.Itoa(c.Writer.Status())

		metrics.IncHTTPRequest(method, path, code)
		metrics.ObserveHTTPDuration(method, path, duration)
	}
}
