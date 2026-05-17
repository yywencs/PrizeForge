package cdc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultHTTPTimeout = 5 * time.Second

type ESWriter struct {
	baseURL string
	client  *http.Client
}

func NewESWriter(cfg *Config) *ESWriter {
	return &ESWriter{
		baseURL: cfg.ESAddr,
		client: &http.Client{
			Timeout: defaultHTTPTimeout,
		},
	}
}

func (w *ESWriter) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.baseURL, nil)
	if err != nil {
		return fmt.Errorf("build es ping request: %w", err)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("ping elasticsearch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return fmt.Errorf("ping elasticsearch status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return nil
}

func (w *ESWriter) Upsert(ctx context.Context, index string, id string, doc any) error {
	payload, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal es document: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, w.documentURL(index, id), bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build es upsert request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	if err := w.do(req); err != nil {
		return fmt.Errorf("upsert elasticsearch document: %w", err)
	}

	return nil
}

func (w *ESWriter) Delete(ctx context.Context, index string, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, w.documentURL(index, id), nil)
	if err != nil {
		return fmt.Errorf("build es delete request: %w", err)
	}

	if err := w.do(req); err != nil {
		return fmt.Errorf("delete elasticsearch document: %w", err)
	}

	return nil
}

func (w *ESWriter) do(req *http.Request) error {
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound && req.Method == http.MethodDelete {
		return nil
	}

	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return nil
}

func (w *ESWriter) documentURL(index string, id string) string {
	return fmt.Sprintf("%s/%s/_doc/%s", w.baseURL, url.PathEscape(index), url.PathEscape(id))
}
