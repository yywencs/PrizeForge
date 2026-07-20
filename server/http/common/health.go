package common

import (
	"context"
	"net/http"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
)

const readinessTimeout = 5 * time.Second

// ReadinessCheck 验证处理流量所需的某个依赖是否可用。
type ReadinessCheck func(context.Context) error

// ReadinessChecks 将依赖名称映射到对应的就绪检查。
type ReadinessChecks map[string]ReadinessCheck

// RegisterHealthRoutes 添加进程存活检查和依赖就绪检查端点。
func RegisterHealthRoutes(engine *gin.Engine, checks ReadinessChecks) {
	engine.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	engine.GET("/readyz", readinessHandler(checks, readinessTimeout))
}

type readinessResult struct {
	name string
	err  error
}

func readinessHandler(checks ReadinessChecks, timeout time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
		defer cancel()

		results := make(chan readinessResult, len(checks))
		pendingChecks := make(map[string]struct{}, len(checks))
		for name, check := range checks {
			pendingChecks[name] = struct{}{}
			go func() {
				var err error
				if check == nil {
					err = context.Canceled
				} else {
					err = check(ctx)
				}
				results <- readinessResult{name: name, err: err}
			}()
		}

		failedChecks := make([]string, 0)
		for len(pendingChecks) > 0 {
			select {
			case result := <-results:
				delete(pendingChecks, result.name)
				if result.err != nil {
					failedChecks = append(failedChecks, result.name)
				}
			case <-ctx.Done():
				for name := range pendingChecks {
					failedChecks = append(failedChecks, name)
					delete(pendingChecks, name)
				}
			}
		}

		if len(failedChecks) > 0 {
			sort.Strings(failedChecks)
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status":        "not_ready",
				"failed_checks": failedChecks,
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	}
}
