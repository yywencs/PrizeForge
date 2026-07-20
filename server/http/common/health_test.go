package common

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestHealthzIsIndependentFromReadiness(t *testing.T) {
	engine := newHealthTestEngine(ReadinessChecks{
		"mysql": func(context.Context) error { return errors.New("unavailable") },
	})

	response := performHealthRequest(engine, "/healthz")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}

	assertJSONBody(t, response, map[string]any{"status": "ok"})
}

func TestReadyzReturnsReadyWhenAllChecksPass(t *testing.T) {
	engine := newHealthTestEngine(ReadinessChecks{
		"mysql": func(context.Context) error { return nil },
		"redis": func(context.Context) error { return nil },
	})

	response := performHealthRequest(engine, "/readyz")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}

	assertJSONBody(t, response, map[string]any{"status": "ready"})
}

func TestReadyzReportsFailedChecksWithoutErrorDetails(t *testing.T) {
	engine := newHealthTestEngine(ReadinessChecks{
		"redis":   func(context.Context) error { return errors.New("redis connection details") },
		"mysql":   func(context.Context) error { return errors.New("mysql connection details") },
		"healthy": func(context.Context) error { return nil },
	})

	response := performHealthRequest(engine, "/readyz")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}

	assertJSONBody(t, response, map[string]any{
		"status":        "not_ready",
		"failed_checks": []any{"mysql", "redis"},
	})
}

func TestReadyzTimesOutSlowChecks(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.GET("/readyz", readinessHandler(ReadinessChecks{
		"slow": func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}, 10*time.Millisecond))

	response := performHealthRequest(engine, "/readyz")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}

	assertJSONBody(t, response, map[string]any{
		"status":        "not_ready",
		"failed_checks": []any{"slow"},
	})
}

func newHealthTestEngine(checks ReadinessChecks) *gin.Engine {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	RegisterHealthRoutes(engine, checks)
	return engine
}

func performHealthRequest(engine http.Handler, path string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	engine.ServeHTTP(response, request)
	return response
}

func assertJSONBody(t *testing.T, response *httptest.ResponseRecorder, want map[string]any) {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("body = %#v, want %#v", got, want)
	}
}
