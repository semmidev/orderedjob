package orderedjob_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/semmidev/orderedjob"
	"github.com/stretchr/testify/assert"
)

func TestFunctionalEventListener(t *testing.T) {
	var jobFailedCalled atomic.Bool
	var chainBlockedCalled atomic.Bool
	var dlqExceededCalled atomic.Bool

	listener := &orderedjob.FunctionalEventListener{
		OnJobFailedFunc: func(ctx context.Context, job orderedjob.Job, err error) {
			jobFailedCalled.Store(true)
		},
		OnChainBlockedFunc: func(ctx context.Context, chainID string, job orderedjob.Job) {
			chainBlockedCalled.Store(true)
		},
		OnDLQThresholdExceededFunc: func(ctx context.Context, dlqCount int64) {
			dlqExceededCalled.Store(true)
		},
	}

	ctx := context.Background()
	dummyJob := orderedjob.Job{ID: uuid.New(), ChainID: "c1", Type: "t1"}

	listener.OnJobFailed(ctx, dummyJob, errors.New("fail"))
	listener.OnChainBlocked(ctx, "c1", dummyJob)
	listener.OnDLQThresholdExceeded(ctx, 10)

	assert.True(t, jobFailedCalled.Load())
	assert.True(t, chainBlockedCalled.Load())
	assert.True(t, dlqExceededCalled.Load())
}

func TestWebhookDispatcher_JSON(t *testing.T) {
	var receivedPayload orderedjob.WebhookPayload
	var receivedHeader string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeader = r.Header.Get("X-Custom-Header")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &receivedPayload)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dispatcher := orderedjob.NewWebhookDispatcher(orderedjob.WebhookConfig{
		URL:     server.URL,
		Headers: map[string]string{"X-Custom-Header": "TestValue"},
	})

	ctx := context.Background()
	dummyJob := orderedjob.Job{ID: uuid.New(), ChainID: "chain-webhook", Type: "EmailJob"}

	dispatcher.OnJobFailed(ctx, dummyJob, errors.New("connection reset"))

	assert.Equal(t, "TestValue", receivedHeader)
	assert.Equal(t, "OnJobFailed", receivedPayload.Event)
	assert.Equal(t, dummyJob.ID.String(), receivedPayload.JobID)
	assert.Equal(t, "chain-webhook", receivedPayload.ChainID)
	assert.Equal(t, "connection reset", receivedPayload.Error)
}

func TestWebhookDispatcher_Slack(t *testing.T) {
	var receivedMap map[string]string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &receivedMap)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dispatcher := orderedjob.NewWebhookDispatcher(orderedjob.WebhookConfig{
		URL:         server.URL,
		SlackFormat: true,
	})

	ctx := context.Background()
	dummyJob := orderedjob.Job{ID: uuid.New(), ChainID: "slack-chain", Type: "PaymentJob"}

	dispatcher.OnChainBlocked(ctx, "slack-chain", dummyJob)

	assert.Contains(t, receivedMap["text"], "OnChainBlocked")
	assert.Contains(t, receivedMap["text"], "slack-chain")
}
