package middleware

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
