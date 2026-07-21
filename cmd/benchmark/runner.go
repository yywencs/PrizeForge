package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const maxResponseBytes = 1 << 20

type drawRequest struct {
	UserID     string `json:"user_id"`
	ActivityID int64  `json:"activity_id"`
	RequestID  string `json:"request_id"`
}

type drawResponse struct {
	Code int    `json:"code"`
	Info string `json:"info"`
	Data struct {
		AwardID    int64  `json:"award_id"`
		AwardTitle string `json:"award_title"`
		AwardIndex int    `json:"award_index"`
	} `json:"data"`
}

type benchmarkRunner struct {
	config   benchmarkConfig
	client   *http.Client
	endpoint string
	runID    string
	sequence atomic.Uint64
}

func newBenchmarkRunner(config benchmarkConfig, client *http.Client) *benchmarkRunner {
	if client == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.MaxIdleConns = config.Concurrency * 2
		transport.MaxIdleConnsPerHost = config.Concurrency
		transport.MaxConnsPerHost = config.Concurrency
		client = &http.Client{
			Transport: transport,
			Timeout:   config.Timeout,
		}
	}

	return &benchmarkRunner{
		config:   config,
		client:   client,
		endpoint: config.endpoint(),
		runID:    strconv.FormatInt(time.Now().UnixNano(), 36),
	}
}

func (r *benchmarkRunner) run(ctx context.Context) benchmarkSummary {
	startedAt := time.Now()
	stopAt := startedAt.Add(r.config.Duration)
	start := make(chan struct{})
	workerStats := make(chan benchmarkStats, r.config.Concurrency)

	var workers sync.WaitGroup
	workers.Add(r.config.Concurrency)
	for range r.config.Concurrency {
		go func() {
			defer workers.Done()
			<-start

			stats := benchmarkStats{}
			for time.Now().Before(stopAt) {
				if ctx.Err() != nil {
					break
				}
				sequence := r.sequence.Add(1)
				stats.record(r.execute(ctx, sequence))
			}
			workerStats <- stats
		}()
	}

	close(start)
	workers.Wait()
	close(workerStats)

	combined := benchmarkStats{}
	for stats := range workerStats {
		combined.merge(stats)
	}
	return summarize(combined, time.Since(startedAt))
}

func (r *benchmarkRunner) execute(ctx context.Context, sequence uint64) requestResult {
	startedAt := time.Now()
	userIndex := (sequence - 1) % uint64(r.config.Users)
	payload := drawRequest{
		UserID:     benchmarkUserID(r.config.UserPrefix, int(userIndex+1)),
		ActivityID: r.config.ActivityID,
		RequestID:  fmt.Sprintf("benchmark-%s-%s", r.runID, strconv.FormatUint(sequence, 36)),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return requestResult{latency: time.Since(startedAt), outcome: outcomeDecodeError}
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint, bytes.NewReader(body))
	if err != nil {
		return requestResult{latency: time.Since(startedAt), outcome: outcomeTransportError}
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")

	response, err := r.client.Do(request)
	if err != nil {
		return requestResult{latency: time.Since(startedAt), outcome: outcomeTransportError}
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	latency := time.Since(startedAt)
	if err != nil || len(responseBody) > maxResponseBytes {
		return requestResult{latency: latency, outcome: outcomeDecodeError}
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return requestResult{latency: latency, outcome: outcomeHTTPError}
	}

	var result drawResponse
	if err := json.Unmarshal(responseBody, &result); err != nil {
		return requestResult{latency: latency, outcome: outcomeDecodeError}
	}
	if result.Code != 0 {
		return requestResult{
			latency:      latency,
			outcome:      outcomeBusinessError,
			businessCode: result.Code,
		}
	}
	return requestResult{latency: latency, outcome: outcomeSuccess}
}
