package orderedjob

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type WebhookConfig struct {
	URL         string            `json:"url"`
	Secret      string            `json:"secret,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	SlackFormat bool              `json:"slack_format,omitempty"`
	HTTPClient  *http.Client      `json:"-"`
}

type WebhookPayload struct {
	Event     string    `json:"event"`
	Timestamp time.Time `json:"timestamp"`
	ChainID   string    `json:"chain_id,omitempty"`
	JobID     string    `json:"job_id,omitempty"`
	JobType   string    `json:"job_type,omitempty"`
	Error     string    `json:"error,omitempty"`
	DLQCount  int64     `json:"dlq_count,omitempty"`
}

type WebhookDispatcher struct {
	config WebhookConfig
	client *http.Client
}

func NewWebhookDispatcher(cfg WebhookConfig) *WebhookDispatcher {
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &WebhookDispatcher{
		config: cfg,
		client: client,
	}
}

func (w *WebhookDispatcher) dispatch(ctx context.Context, payload WebhookPayload) {
	if w.config.URL == "" {
		return
	}

	var bodyBytes []byte
	var err error

	if w.config.SlackFormat {
		text := fmt.Sprintf("⚠️ *[orderedjob Alert]* Event: `%s`", payload.Event)
		if payload.JobID != "" {
			text += fmt.Sprintf("\n• Job ID: `%s` (%s)", payload.JobID, payload.JobType)
		}
		if payload.ChainID != "" {
			text += fmt.Sprintf("\n• Chain ID: `%s`", payload.ChainID)
		}
		if payload.Error != "" {
			text += fmt.Sprintf("\n• Error: `%s`", payload.Error)
		}
		if payload.DLQCount > 0 {
			text += fmt.Sprintf("\n• DLQ Count: `%d`", payload.DLQCount)
		}
		slackObj := map[string]string{"text": text}
		bodyBytes, err = json.Marshal(slackObj)
	} else {
		bodyBytes, err = json.Marshal(payload)
	}
	if err != nil {
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.config.URL, bytes.NewReader(bodyBytes))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range w.config.Headers {
		req.Header.Set(k, v)
	}

	resp, err := w.client.Do(req)
	if err == nil && resp != nil {
		_ = resp.Body.Close()
	}
}

func (w *WebhookDispatcher) OnJobFailed(ctx context.Context, job Job, err error) {
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	w.dispatch(ctx, WebhookPayload{
		Event:     "OnJobFailed",
		Timestamp: time.Now().UTC(),
		JobID:     job.ID.String(),
		ChainID:   job.ChainID,
		JobType:   job.Type,
		Error:     errStr,
	})
}

func (w *WebhookDispatcher) OnChainBlocked(ctx context.Context, chainID string, job Job) {
	w.dispatch(ctx, WebhookPayload{
		Event:     "OnChainBlocked",
		Timestamp: time.Now().UTC(),
		JobID:     job.ID.String(),
		ChainID:   chainID,
		JobType:   job.Type,
	})
}

func (w *WebhookDispatcher) OnDLQThresholdExceeded(ctx context.Context, dlqCount int64) {
	w.dispatch(ctx, WebhookPayload{
		Event:     "OnDLQThresholdExceeded",
		Timestamp: time.Now().UTC(),
		DLQCount:  dlqCount,
	})
}
