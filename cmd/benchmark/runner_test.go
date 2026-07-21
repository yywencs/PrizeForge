package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func jsonResponseClient(handler func(*http.Request) string) *http.Client {
	return &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(handler(request))),
				Request:    request,
			}, nil
		}),
	}
}

// TestBenchmarkRunnerExecuteBuildsDynamicDrawRequest 验证压测请求包含轮转用户、活动 ID 和唯一 request_id，并识别业务成功。
func TestBenchmarkRunnerExecuteBuildsDynamicDrawRequest(t *testing.T) {
	requests := make([]drawRequest, 0, 2)
	client := jsonResponseClient(func(request *http.Request) string {
		if request.Method != http.MethodPost || request.URL.Path != drawPath {
			t.Errorf("request = %s %s, want POST %s", request.Method, request.URL.Path, drawPath)
		}
		var payload drawRequest
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Errorf("decode request: %v", err)
		}
		requests = append(requests, payload)
		return `{"code":0,"info":"success","data":{"award_id":101}}`
	})

	config := benchmarkConfig{
		BaseURL:     "http://example.test",
		ActivityID:  100301,
		Users:       2,
		Concurrency: 1,
		Duration:    time.Second,
		Timeout:     time.Second,
		UserPrefix:  "load-user",
	}
	runner := newBenchmarkRunner(config, client)
	firstResult := runner.execute(context.Background(), 1)
	secondResult := runner.execute(context.Background(), 2)

	if firstResult.outcome != outcomeSuccess || secondResult.outcome != outcomeSuccess {
		t.Fatalf("outcomes = %v/%v, want success/success", firstResult.outcome, secondResult.outcome)
	}
	firstRequest := requests[0]
	secondRequest := requests[1]
	if firstRequest.UserID != "load-user-000001" || secondRequest.UserID != "load-user-000002" {
		t.Fatalf("user IDs = %q/%q, want rotating users", firstRequest.UserID, secondRequest.UserID)
	}
	if firstRequest.ActivityID != 100301 || secondRequest.ActivityID != 100301 {
		t.Fatalf("activity IDs = %d/%d, want 100301", firstRequest.ActivityID, secondRequest.ActivityID)
	}
	if firstRequest.RequestID == "" || firstRequest.RequestID == secondRequest.RequestID {
		t.Fatalf("request IDs = %q/%q, want unique non-empty values", firstRequest.RequestID, secondRequest.RequestID)
	}
}

// TestBenchmarkRunnerExecuteClassifiesBusinessError 验证 HTTP 200 中的非零业务码不会被误计为成功。
func TestBenchmarkRunnerExecuteClassifiesBusinessError(t *testing.T) {
	client := jsonResponseClient(func(*http.Request) string {
		return `{"code":409,"info":"draw in progress","data":null}`
	})

	config := benchmarkConfig{
		BaseURL:     "http://example.test",
		ActivityID:  100301,
		Users:       1,
		Concurrency: 1,
		Duration:    time.Second,
		Timeout:     time.Second,
		UserPrefix:  "load-user",
	}
	result := newBenchmarkRunner(config, client).execute(context.Background(), 1)
	if result.outcome != outcomeBusinessError || result.businessCode != 409 {
		t.Fatalf("result = %+v, want business error code 409", result)
	}
}
