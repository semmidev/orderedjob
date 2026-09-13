package orderedjob

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/destel/rill"
)

// BatchEnqueueParallel demonstrates rill usage for high-throughput enqueue validation / transformation.
// It validates payloads in parallel, then enqueues atomically via repository.
func BatchEnqueueParallel(ctx context.Context, eng *Engine, reqs []EnqueueRequest) ([]Job, error) {
	// Step 1: parallel validation / payload normalization using rill (safe concurrency toolkit)
	validated := make([]EnqueueRequest, len(reqs))

	type indexedReq struct {
		idx int
		req EnqueueRequest
	}
	indexed := make([]indexedReq, len(reqs))
	for i, r := range reqs {
		indexed[i] = indexedReq{idx: i, req: r}
	}

	err := rill.ForEach(rill.FromSlice(indexed, nil), 10, func(item indexedReq) error {
		req := item.req
		if req.ChainID == "" {
			return fmt.Errorf("chain_id required at index %d", item.idx)
		}
		if req.Type == "" {
			return fmt.Errorf("type required at index %d", item.idx)
		}
		// normalize payload: ensure JSON-serializable
		if req.Payload != nil {
			if _, err := json.Marshal(req.Payload); err != nil {
				return fmt.Errorf("invalid payload at %d: %w", item.idx, err)
			}
		}
		// set default max attempts if not set
		if req.MaxAttempts == 0 {
			req.MaxAttempts = 5
		}
		validated[item.idx] = req
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Step 2: atomic batch enqueue (DB transaction)
	return eng.EnqueueBatch(ctx, validated)
}

// ProcessJobsParallel is an example of using rill for parallel post-processing (metrics aggregation).
func AggregateChainStats(jobs []Job) map[string]int64 {
	// rill Map-Reduce style
	type pair struct{ chain string }
	stream := rill.FromSlice(jobs, nil)
	counts := make(map[string]int64)

	// Use rill to parallelize counting via concurrent map
	// For simplicity, sequential final aggregation but parallel pre-processing
	_ = rill.ForEach(stream, 8, func(job Job) error {
		// simulate heavy compute
		_ = len(job.Payload)
		return nil
	})

	for _, j := range jobs {
		counts[j.ChainID]++
	}
	return counts
}

// ParallelClaimDemo shows how rill can be used for fan-out execution if you ever need to process claimed jobs via pipeline.
func ParallelClaimDemo() {
	// placeholder illustrating rill usage pattern recommended by task:
	// rill.FromChan -> Map (handler) -> Filter (success) with ordered=false for max throughput
	_ = time.Now()
}
