package ui_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/semmidev/orderedjob"
	"github.com/semmidev/orderedjob/repository/memory"
	"github.com/semmidev/orderedjob/ui"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUIHandler_Endpoints(t *testing.T) {
	ctx := context.Background()
	repo := memory.New()

	// Enqueue test jobs
	j1, err := repo.Enqueue(ctx, orderedjob.EnqueueRequest{
		ChainID:  "chain-ui-1",
		Sequence: 1,
		Type:     "EmailJob",
		Payload:  map[string]string{"email": "user@example.com"},
	})
	require.NoError(t, err)

	_, err = repo.Enqueue(ctx, orderedjob.EnqueueRequest{
		ChainID:  "chain-ui-1",
		Sequence: 2,
		Type:     "SMSJob",
		Payload:  map[string]string{"phone": "+628123456789"},
	})
	require.NoError(t, err)

	handler := ui.NewHandler(repo, ui.WithRootPath("/ui"))

	t.Run("GET /ui/ Index HTML", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ui/", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "OrderedJob")
		assert.Contains(t, rec.Body.String(), "Monitoring")
	})

	t.Run("GET /ui/api/stats", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ui/api/stats", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var stats orderedjob.Stats
		err := json.NewDecoder(rec.Body).Decode(&stats)
		require.NoError(t, err)
		assert.Equal(t, int64(2), stats.Total)
		assert.Equal(t, int64(1), stats.Pending)
		assert.Equal(t, int64(1), stats.Blocked)
		assert.Equal(t, int64(1), stats.ActiveChains)
	})

	t.Run("GET /ui/api/jobs", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ui/api/jobs?chain_id=chain-ui-1", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		err := json.NewDecoder(rec.Body).Decode(&resp)
		require.NoError(t, err)
		assert.Equal(t, float64(2), resp["total"])
	})

	t.Run("GET /ui/api/jobs/{id}", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ui/api/jobs/"+j1.ID.String(), nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var j orderedjob.Job
		err := json.NewDecoder(rec.Body).Decode(&j)
		require.NoError(t, err)
		assert.Equal(t, j1.ID, j.ID)
		assert.Equal(t, "EmailJob", j.Type)
	})

	t.Run("GET /ui/api/chains", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ui/api/chains", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp struct {
			Chains     []orderedjob.ChainSummary `json:"chains"`
			Total      int64                     `json:"total"`
			Page       int                       `json:"page"`
			Limit      int                       `json:"limit"`
			TotalPages int64                     `json:"total_pages"`
		}
		err := json.NewDecoder(rec.Body).Decode(&resp)
		require.NoError(t, err)
		require.Len(t, resp.Chains, 1)
		assert.Equal(t, "chain-ui-1", resp.Chains[0].ChainID)
		assert.Equal(t, int64(2), resp.Chains[0].TotalJobs)
		assert.Equal(t, int64(1), resp.Total)
	})

	t.Run("POST /ui/api/enqueue", func(t *testing.T) {
		body := map[string]any{
			"chain_id": "chain-ui-2",
			"sequence": 1,
			"job_type": "NewTestJob",
			"payload":  map[string]int{"val": 42},
		}
		b, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "/ui/api/enqueue", bytes.NewReader(b))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var j orderedjob.Job
		err := json.NewDecoder(rec.Body).Decode(&j)
		require.NoError(t, err)
		assert.Equal(t, "chain-ui-2", j.ChainID)
		assert.Equal(t, "NewTestJob", j.Type)
	})

	t.Run("POST /ui/api/jobs/{id}/cancel", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/ui/api/jobs/"+j1.ID.String()+"/cancel", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		j, err := repo.Get(ctx, j1.ID)
		require.NoError(t, err)
		assert.Equal(t, "CANCEL_REQUESTED", j.Status)
	})

	t.Run("POST /ui/api/jobs/{id}/skip", func(t *testing.T) {
		jFailed, err := repo.Enqueue(ctx, orderedjob.EnqueueRequest{
			ChainID:  "chain-ui-skip",
			Sequence: 1,
			Type:     "FailedJob",
		})
		require.NoError(t, err)
		_ = repo.Fail(ctx, jFailed.ID, "", 0, "test failure", true)

		req := httptest.NewRequest(http.MethodPost, "/ui/api/jobs/"+jFailed.ID.String()+"/skip", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		j, err := repo.Get(ctx, jFailed.ID)
		require.NoError(t, err)
		assert.Equal(t, "COMPLETED", j.Status)
	})

	t.Run("POST /ui/api/jobs/{id}/replay", func(t *testing.T) {
		jReplay, err := repo.Enqueue(ctx, orderedjob.EnqueueRequest{
			ChainID:  "chain-ui-replay",
			Sequence: 1,
			Type:     "ReplayJob",
		})
		require.NoError(t, err)
		_ = repo.Fail(ctx, jReplay.ID, "", 0, "test failure", true)

		req := httptest.NewRequest(http.MethodPost, "/ui/api/jobs/"+jReplay.ID.String()+"/replay", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		j, err := repo.Get(ctx, jReplay.ID)
		require.NoError(t, err)
		assert.Equal(t, "PENDING", j.Status)
	})

	t.Run("POST /ui/api/jobs/{id}/reschedule", func(t *testing.T) {
		jResched, err := repo.Enqueue(ctx, orderedjob.EnqueueRequest{
			ChainID:  "chain-ui-resched",
			Sequence: 1,
			Type:     "ReschedJob",
		})
		require.NoError(t, err)

		body := map[string]any{
			"available_at": "2030-01-01T00:00:00Z",
		}
		b, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "/ui/api/jobs/"+jResched.ID.String()+"/reschedule", bytes.NewReader(b))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		j, err := repo.Get(ctx, jResched.ID)
		require.NoError(t, err)
		assert.Equal(t, 2030, j.AvailableAt.Year())
	})

	t.Run("POST /ui/api/jobs/{id}/replay with payload", func(t *testing.T) {
		repoLocal := memory.New()
		handlerLocal := ui.NewHandler(repoLocal, ui.WithRootPath("/ui"))

		jPayload, err := repoLocal.Enqueue(ctx, orderedjob.EnqueueRequest{
			ChainID:  "chain-ui-payload-replay",
			Sequence: 1,
			Type:     "PayloadReplayJob",
			Payload:  map[string]string{"old": "data"},
		})
		require.NoError(t, err)

		claimed, err := repoLocal.Claim(ctx, "w-test", 10)
		require.NoError(t, err)
		err = repoLocal.Fail(ctx, claimed.ID, claimed.WorkerID, claimed.LeaseGeneration, "test err", true)
		require.NoError(t, err)

		body := map[string]any{
			"payload": map[string]string{"new": "data"},
		}
		b, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "/ui/api/jobs/"+jPayload.ID.String()+"/replay", bytes.NewReader(b))
		rec := httptest.NewRecorder()
		handlerLocal.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		j, err := repoLocal.Get(ctx, jPayload.ID)
		require.NoError(t, err)
		assert.Equal(t, "PENDING", j.Status)
		assert.JSONEq(t, `{"new":"data"}`, string(j.Payload))
	})

	t.Run("GET /ui/api/jobs with tenant_id and trace_id filters", func(t *testing.T) {
		_, err := repo.Enqueue(ctx, orderedjob.EnqueueRequest{
			ChainID:  "chain-ui-tenant",
			Sequence: 1,
			Type:     "TenantJob",
			TenantID: "tenant-ui-123",
			TraceID:  "trace-ui-456",
		})
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/ui/api/jobs?tenant_id=tenant-ui-123&trace_id=trace-ui-456", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		err = json.NewDecoder(rec.Body).Decode(&resp)
		require.NoError(t, err)
		assert.Equal(t, float64(1), resp["total"])
	})

	t.Run("GET /ui/api/events (SSE stream)", func(t *testing.T) {
		reqCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		req := httptest.NewRequest(http.MethodGet, "/ui/api/events", nil).WithContext(reqCtx)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)
		assert.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
		assert.Contains(t, rec.Body.String(), "event: stats")
	})

	t.Run("POST /ui/api/dlq/bulk-replay, bulk-skip, bulk-purge", func(t *testing.T) {
		repoLocal := memory.New()
		handlerLocal := ui.NewHandler(repoLocal, ui.WithRootPath("/ui"))

		jDLQ, err := repoLocal.Enqueue(ctx, orderedjob.EnqueueRequest{
			ChainID:  "chain-bulk-api",
			Sequence: 1,
			Type:     "BulkJob",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, jDLQ.ID)

		claimed, err := repoLocal.Claim(ctx, "w1", 10)
		require.NoError(t, err)
		_ = repoLocal.Fail(ctx, claimed.ID, claimed.WorkerID, claimed.LeaseGeneration, "err", true)

		// Bulk Replay
		bReplay, _ := json.Marshal(map[string]string{"chain_id": "chain-bulk-api"})
		req := httptest.NewRequest(http.MethodPost, "/ui/api/dlq/bulk-replay", bytes.NewReader(bReplay))
		rec := httptest.NewRecorder()
		handlerLocal.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "replayed successfully")

		// Re-fail & Bulk Skip
		claimed, _ = repoLocal.Claim(ctx, "w1", 10)
		_ = repoLocal.Fail(ctx, claimed.ID, claimed.WorkerID, claimed.LeaseGeneration, "err", true)

		bSkip, _ := json.Marshal(map[string]string{"job_type": "BulkJob"})
		req = httptest.NewRequest(http.MethodPost, "/ui/api/dlq/bulk-skip", bytes.NewReader(bSkip))
		rec = httptest.NewRecorder()
		handlerLocal.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "skipped successfully")

		// Re-enqueue, fail & Bulk Purge
		jPurge, _ := repoLocal.Enqueue(ctx, orderedjob.EnqueueRequest{ChainID: "chain-purge", Sequence: 1, Type: "PurgeJob"})
		claimedPurge, _ := repoLocal.Claim(ctx, "w1", 10)
		_ = repoLocal.Fail(ctx, claimedPurge.ID, claimedPurge.WorkerID, claimedPurge.LeaseGeneration, "err", true)

		bPurge, _ := json.Marshal(map[string]string{"job_type": "PurgeJob"})
		req = httptest.NewRequest(http.MethodPost, "/ui/api/dlq/bulk-purge", bytes.NewReader(bPurge))
		rec = httptest.NewRecorder()
		handlerLocal.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "purged successfully")

		_, err = repoLocal.Get(ctx, jPurge.ID)
		assert.ErrorIs(t, err, orderedjob.ErrNotFound)
	})
}

func TestUIHandler_Authentication(t *testing.T) {
	repo := memory.New()

	t.Run("Basic Auth", func(t *testing.T) {
		h := ui.New(repo, ui.WithBasicAuth("admin", "secret123"))

		// No credentials
		req := httptest.NewRequest(http.MethodGet, "/ui/api/stats", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
		assert.Equal(t, `Basic realm="OrderedJob Dashboard"`, rec.Header().Get("WWW-Authenticate"))

		// Invalid credentials
		req = httptest.NewRequest(http.MethodGet, "/ui/api/stats", nil)
		req.SetBasicAuth("admin", "wrongpass")
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)

		// Valid credentials
		req = httptest.NewRequest(http.MethodGet, "/ui/api/stats", nil)
		req.SetBasicAuth("admin", "secret123")
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("Token Auth", func(t *testing.T) {
		h := ui.New(repo, ui.WithTokenAuth("my-secret-token"))

		// Unauthorized request
		req := httptest.NewRequest(http.MethodGet, "/ui/api/stats", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)

		// Header Bearer Token
		req = httptest.NewRequest(http.MethodGet, "/ui/api/stats", nil)
		req.Header.Set("Authorization", "Bearer my-secret-token")
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Query parameter token (for SSE/EventSource support)
		req = httptest.NewRequest(http.MethodGet, "/ui/api/stats?token=my-secret-token", nil)
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("JWT Auth", func(t *testing.T) {
		secret := "my-jwt-secret-key-123"
		h := ui.New(repo, ui.WithJWTAuth(secret))

		token, err := ui.GenerateHS256JWT(secret, map[string]any{"sub": "admin"}, 10*time.Minute)
		require.NoError(t, err)

		// Unauthorized
		req := httptest.NewRequest(http.MethodGet, "/ui/api/stats", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)

		// Valid JWT Token in Header
		req = httptest.NewRequest(http.MethodGet, "/ui/api/stats", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Expired JWT Token
		expiredToken, err := ui.GenerateHS256JWT(secret, map[string]any{"sub": "admin"}, -1*time.Minute)
		require.NoError(t, err)
		req = httptest.NewRequest(http.MethodGet, "/ui/api/stats", nil)
		req.Header.Set("Authorization", "Bearer "+expiredToken)
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("JWT Validator & AuthFunc", func(t *testing.T) {
		h := ui.New(repo, ui.WithJWTValidator(func(tokenStr string) bool {
			return tokenStr == "custom-valid-jwt"
		}))

		req := httptest.NewRequest(http.MethodGet, "/ui/api/stats", nil)
		req.Header.Set("Authorization", "Bearer custom-valid-jwt")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Custom AuthFunc
		hFunc := ui.New(repo, ui.WithAuthFunc(func(r *http.Request) bool {
			return r.Header.Get("X-Custom-Auth") == "allowed"
		}))

		req = httptest.NewRequest(http.MethodGet, "/ui/api/stats", nil)
		req.Header.Set("X-Custom-Auth", "allowed")
		rec = httptest.NewRecorder()
		hFunc.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}
