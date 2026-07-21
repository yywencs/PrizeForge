package main

import (
	"fmt"
	"math"
	"sort"
	"time"
)

type outcome uint8

const (
	outcomeSuccess outcome = iota
	outcomeTransportError
	outcomeHTTPError
	outcomeDecodeError
	outcomeBusinessError
)

type requestResult struct {
	latency      time.Duration
	outcome      outcome
	businessCode int
}

type benchmarkStats struct {
	total           int64
	success         int64
	transportErrors int64
	httpErrors      int64
	decodeErrors    int64
	businessErrors  int64
	latencies       []time.Duration
	businessCodes   map[int]int64
}

func (s *benchmarkStats) record(result requestResult) {
	s.total++
	s.latencies = append(s.latencies, result.latency)

	switch result.outcome {
	case outcomeSuccess:
		s.success++
	case outcomeTransportError:
		s.transportErrors++
	case outcomeHTTPError:
		s.httpErrors++
	case outcomeDecodeError:
		s.decodeErrors++
	case outcomeBusinessError:
		s.businessErrors++
		if s.businessCodes == nil {
			s.businessCodes = make(map[int]int64)
		}
		s.businessCodes[result.businessCode]++
	}
}

func (s *benchmarkStats) merge(other benchmarkStats) {
	s.total += other.total
	s.success += other.success
	s.transportErrors += other.transportErrors
	s.httpErrors += other.httpErrors
	s.decodeErrors += other.decodeErrors
	s.businessErrors += other.businessErrors
	s.latencies = append(s.latencies, other.latencies...)
	if len(other.businessCodes) > 0 && s.businessCodes == nil {
		s.businessCodes = make(map[int]int64)
	}
	for code, count := range other.businessCodes {
		s.businessCodes[code] += count
	}
}

type benchmarkSummary struct {
	Elapsed        time.Duration
	Stats          benchmarkStats
	RequestsPerSec float64
	SuccessRate    float64
	AverageLatency time.Duration
	P50Latency     time.Duration
	P95Latency     time.Duration
	P99Latency     time.Duration
	MaximumLatency time.Duration
}

func summarize(stats benchmarkStats, elapsed time.Duration) benchmarkSummary {
	summary := benchmarkSummary{Elapsed: elapsed, Stats: stats}
	if elapsed > 0 {
		summary.RequestsPerSec = float64(stats.total) / elapsed.Seconds()
	}
	if stats.total > 0 {
		summary.SuccessRate = float64(stats.success) / float64(stats.total) * 100
	}
	if len(stats.latencies) == 0 {
		return summary
	}

	sorted := append([]time.Duration(nil), stats.latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var totalLatency time.Duration
	for _, latency := range sorted {
		totalLatency += latency
	}
	summary.AverageLatency = totalLatency / time.Duration(len(sorted))
	summary.P50Latency = percentile(sorted, 0.50)
	summary.P95Latency = percentile(sorted, 0.95)
	summary.P99Latency = percentile(sorted, 0.99)
	summary.MaximumLatency = sorted[len(sorted)-1]
	return summary
}

func percentile(sorted []time.Duration, quantile float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(float64(len(sorted))*quantile)) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func printSummary(summary benchmarkSummary) {
	fmt.Println()
	fmt.Println("Benchmark result")
	fmt.Printf("  elapsed:          %s\n", summary.Elapsed.Round(time.Millisecond))
	fmt.Printf("  total requests:   %d\n", summary.Stats.total)
	fmt.Printf("  requests/sec:     %.2f\n", summary.RequestsPerSec)
	fmt.Printf("  business success: %d (%.2f%%)\n", summary.Stats.success, summary.SuccessRate)
	fmt.Printf("  business errors:  %d\n", summary.Stats.businessErrors)
	fmt.Printf("  transport errors: %d\n", summary.Stats.transportErrors)
	fmt.Printf("  HTTP errors:      %d\n", summary.Stats.httpErrors)
	fmt.Printf("  decode errors:    %d\n", summary.Stats.decodeErrors)
	fmt.Printf("  latency avg:      %s\n", summary.AverageLatency.Round(time.Microsecond))
	fmt.Printf("  latency p50:      %s\n", summary.P50Latency.Round(time.Microsecond))
	fmt.Printf("  latency p95:      %s\n", summary.P95Latency.Round(time.Microsecond))
	fmt.Printf("  latency p99:      %s\n", summary.P99Latency.Round(time.Microsecond))
	fmt.Printf("  latency max:      %s\n", summary.MaximumLatency.Round(time.Microsecond))

	if len(summary.Stats.businessCodes) > 0 {
		codes := make([]int, 0, len(summary.Stats.businessCodes))
		for code := range summary.Stats.businessCodes {
			codes = append(codes, code)
		}
		sort.Ints(codes)
		fmt.Println("  business codes:")
		for _, code := range codes {
			fmt.Printf("    code=%d count=%d\n", code, summary.Stats.businessCodes[code])
		}
	}
}
