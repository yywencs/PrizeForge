package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"prizeforge/server/http/common"
)

func TestServerRegistersHealthRoutes(t *testing.T) {
	server := NewServer(":0", nil, nil, common.ReadinessChecks{
		"dependency": func(context.Context) error { return nil },
	})

	for _, path := range []string{"/healthz", "/readyz"} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, path, nil)
		server.Engine().ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Errorf("GET %s status = %d, want %d", path, response.Code, http.StatusOK)
		}
	}
}
