package ui_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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
		var resp map[string][]orderedjob.ChainSummary
		err := json.NewDecoder(rec.Body).Decode(&resp)
		require.NoError(t, err)
		require.Len(t, resp["chains"], 1)
		assert.Equal(t, "chain-ui-1", resp["chains"][0].ChainID)
		assert.Equal(t, int64(2), resp["chains"][0].TotalJobs)
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
}
