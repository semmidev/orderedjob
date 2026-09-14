package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	orderedjob "github.com/semmidev/orderedjob"
	"github.com/semmidev/orderedjob/lifecycle"
)

type repo struct {
	mu   sync.Mutex
	jobs map[uuid.UUID]orderedjob.Job
	// index chain->seq->id
	chain map[string]map[int64]uuid.UUID
}

func New() *repo {
	return &repo{
		jobs:  make(map[uuid.UUID]orderedjob.Job),
		chain: make(map[string]map[int64]uuid.UUID),
	}
}

func (r *repo) NotifyChannel() string { return "" }

func (r *repo) Enqueue(ctx context.Context, req orderedjob.EnqueueRequest) (orderedjob.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.enqueueSingleInMemory(req)
}

func (r *repo) EnqueueBatch(ctx context.Context, reqs []orderedjob.EnqueueRequest) ([]orderedjob.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	backupJobs := make(map[uuid.UUID]orderedjob.Job, len(r.jobs))
	for k, v := range r.jobs {
		backupJobs[k] = v
	}
	backupChain := make(map[string]map[int64]uuid.UUID, len(r.chain))
	for ch, m := range r.chain {
		nm := make(map[int64]uuid.UUID, len(m))
		for s, id := range m {
			nm[s] = id
		}
		backupChain[ch] = nm
	}

	var out []orderedjob.Job
	for _, req := range reqs {
		job, err := r.enqueueSingleInMemory(req)
		if err != nil {
			r.jobs = backupJobs
			r.chain = backupChain
			return nil, err
		}
		out = append(out, job)
	}
	return out, nil
}

func (r *repo) findJobByIdempotencyKey(key string) (orderedjob.Job, bool) {
	for _, existing := range r.jobs {
		if existing.IdempotencyKey == key {
			return existing, true
		}
	}
	return orderedjob.Job{}, false
}

func (r *repo) getMaxSequence(chain string) int64 {
	max := int64(0)
	if m, ok := r.chain[chain]; ok {
		for s := range m {
			if s > max {
				max = s
			}
		}
	}
	return max
}

func (r *repo) validateAndResolveSequence(chain string, seq int64) (int64, error) {
	if seq == 0 {
		return r.getMaxSequence(chain) + 1, nil
	}

	m, hasChain := r.chain[chain]
	if seq > 1 {
		if hasChain {
			max := r.getMaxSequence(chain)
			if _, ok := m[seq-1]; !ok && seq > max+1 {
				return 0, fmt.Errorf("%w: missing predecessor %d for chain %s", orderedjob.ErrSequenceGap, seq-1, chain)
			}
		} else {
			return 0, fmt.Errorf("%w: first job must be seq 1", orderedjob.ErrSequenceGap)
		}
	}

	if hasChain {
		if _, exists := m[seq]; exists {
			return 0, orderedjob.ErrDuplicateChainSeq
		}
	}

	return seq, nil
}

func (r *repo) determineInitialStatus(chain string, seq int64) string {
	if seq <= 1 {
		return lifecycle.StatePending
	}
	m, ok := r.chain[chain]
	if !ok {
		return lifecycle.StateBlocked
	}
	predID, ok := m[seq-1]
	if !ok {
		return lifecycle.StateBlocked
	}
	pred := r.jobs[predID]
	isPredFinished := pred.Status == lifecycle.StateCompleted ||
		((pred.Status == lifecycle.StateFailed || pred.Status == lifecycle.StateDeadLettered) &&
			(pred.OrderingMode == orderedjob.OrderingModeSkipOnFailure || pred.OrderingMode == orderedjob.OrderingModeDeadLetterAndContinue))
	if !isPredFinished {
		return lifecycle.StateBlocked
	}
	return lifecycle.StatePending
}

func (r *repo) enqueueSingleInMemory(req orderedjob.EnqueueRequest) (orderedjob.Job, error) {
	chain := req.ChainID
	if chain == "" {
		return orderedjob.Job{}, fmt.Errorf("chain_id required")
	}
	if req.IdempotencyKey != "" {
		if existing, ok := r.findJobByIdempotencyKey(req.IdempotencyKey); ok {
			return existing, nil
		}
	}

	seq, err := r.validateAndResolveSequence(chain, req.Sequence)
	if err != nil {
		return orderedjob.Job{}, err
	}

	payloadBytes, _ := json.Marshal(req.Payload)
	if len(payloadBytes) == 0 || string(payloadBytes) == "null" {
		payloadBytes = []byte("{}")
	}

	id := uuid.New()
	now := time.Now().UTC()
	availableAt := now
	if req.AvailableAt != nil {
		availableAt = *req.AvailableAt
	}
	maxAttempts := req.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 5
	}

	orderingMode := req.OrderingMode
	if orderingMode == "" {
		orderingMode = orderedjob.OrderingModeStrict
	}

	status := r.determineInitialStatus(chain, seq)

	job := orderedjob.Job{
		ID:             id,
		ChainID:        chain,
		Sequence:       seq,
		Type:           req.Type,
		Payload:        payloadBytes,
		IdempotencyKey: req.IdempotencyKey,
		TenantID:       req.TenantID,
		TraceID:        req.TraceID,
		OrderingMode:   orderingMode,
		Status:         status,
		Attempt:        0,
		MaxAttempts:    maxAttempts,
		AvailableAt:    availableAt,
		DeadlineAt:     req.DeadlineAt,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	r.jobs[id] = job
	if _, ok := r.chain[chain]; !ok {
		r.chain[chain] = make(map[int64]uuid.UUID)
	}
	r.chain[chain][seq] = id
	return job, nil
}

func (r *repo) Get(ctx context.Context, id uuid.UUID) (orderedjob.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok {
		return orderedjob.Job{}, orderedjob.ErrNotFound
	}
	return j, nil
}

func (r *repo) GetByChainSeq(ctx context.Context, chainID string, seq int64) (orderedjob.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.chain[chainID]; ok {
		if id, ok := m[seq]; ok {
			return r.jobs[id], nil
		}
	}
	return orderedjob.Job{}, orderedjob.ErrNotFound
}

func (r *repo) Claim(ctx context.Context, workerID string, lease time.Duration) (orderedjob.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now().UTC()
	// collect candidates sorted by available_at, created_at
	type cand struct {
		job orderedjob.Job
	}
	var cands []orderedjob.Job
	for _, j := range r.jobs {
		if (j.Status == lifecycle.StatePending || j.Status == lifecycle.StateRetrying) && !j.AvailableAt.After(now) {
			// eligibility: sequence 1 or predecessor completed
			if j.Sequence == 1 {
				cands = append(cands, j)
			} else {
				if m, ok := r.chain[j.ChainID]; ok {
					if predID, ok := m[j.Sequence-1]; ok {
						if pred, ok := r.jobs[predID]; ok {
							isPredFinished := pred.Status == lifecycle.StateCompleted ||
								((pred.Status == lifecycle.StateFailed || pred.Status == lifecycle.StateDeadLettered) &&
									(pred.OrderingMode == orderedjob.OrderingModeSkipOnFailure || pred.OrderingMode == orderedjob.OrderingModeDeadLetterAndContinue))
							if isPredFinished {
								cands = append(cands, j)
							}
						}
					}
				}
			}
		}
	}
	sort.Slice(cands, func(i, j int) bool {
		if !cands[i].AvailableAt.Equal(cands[j].AvailableAt) {
			return cands[i].AvailableAt.Before(cands[j].AvailableAt)
		}
		return cands[i].CreatedAt.Before(cands[j].CreatedAt)
	})
	if len(cands) == 0 {
		return orderedjob.Job{}, orderedjob.ErrNotFound
	}
	job := cands[0]
	// ensure no other job of same chain is PROCESSING (should be enforced by eligibility but double-check)
	for _, j := range r.jobs {
		if j.ChainID == job.ChainID && j.Status == lifecycle.StateProcessing {
			// skip this chain
			// try next candidate
			for _, next := range cands[1:] {
				conflict := false
				for _, jj := range r.jobs {
					if jj.ChainID == next.ChainID && jj.Status == lifecycle.StateProcessing {
						conflict = true
						break
					}
				}
				if !conflict {
					job = next
					goto found
				}
			}
			return orderedjob.Job{}, orderedjob.ErrNotFound
		}
	}
found:
	leaseUntil := now.Add(lease)
	job.Status = lifecycle.StateProcessing
	job.WorkerID = workerID
	job.LeaseGeneration++
	job.LeaseUntil = &leaseUntil
	job.Attempt++
	if job.StartedAt == nil {
		job.StartedAt = &now
	}
	job.UpdatedAt = now
	r.jobs[job.ID] = job
	return job, nil
}

func (r *repo) Complete(ctx context.Context, id uuid.UUID, workerID string, leaseGen int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok {
		return orderedjob.ErrNotFound
	}
	if j.WorkerID != workerID || j.LeaseGeneration != leaseGen {
		return orderedjob.ErrLeaseConflict
	}
	now := time.Now().UTC()
	j.Status = lifecycle.StateCompleted
	j.CompletedAt = &now
	j.UpdatedAt = now
	j.LeaseUntil = nil
	r.jobs[id] = j

	// promote next
	if m, ok := r.chain[j.ChainID]; ok {
		if nextID, ok := m[j.Sequence+1]; ok {
			next := r.jobs[nextID]
			if next.Status == lifecycle.StateBlocked {
				// only promote if current completed
				next.Status = lifecycle.StatePending
				next.UpdatedAt = now
				r.jobs[nextID] = next
			}
		}
	}
	return nil
}

func (r *repo) Fail(ctx context.Context, id uuid.UUID, workerID string, leaseGen int, errMsg string, terminal bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok {
		return orderedjob.ErrNotFound
	}
	if j.WorkerID != workerID || j.LeaseGeneration != leaseGen {
		return orderedjob.ErrLeaseConflict
	}
	now := time.Now().UTC()
	j.LastError = errMsg
	j.UpdatedAt = now
	if terminal {
		if j.OrderingMode == orderedjob.OrderingModeDeadLetterAndContinue {
			j.Status = lifecycle.StateDeadLettered
		} else {
			j.Status = lifecycle.StateFailed
		}
		t := now
		j.FailedAt = &t
		j.LeaseUntil = nil
	} else {
		j.Status = lifecycle.StateFailed
		j.FailedAt = &now
		j.LeaseUntil = nil
	}
	r.jobs[id] = j
	return nil
}

func (r *repo) Retry(ctx context.Context, id uuid.UUID, workerID string, leaseGen int, errMsg string, nextAvailable time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok {
		return orderedjob.ErrNotFound
	}
	if j.WorkerID != workerID || j.LeaseGeneration != leaseGen {
		return orderedjob.ErrLeaseConflict
	}
	now := time.Now().UTC()
	j.Status = lifecycle.StateRetrying
	j.LastError = errMsg
	j.AvailableAt = nextAvailable
	j.UpdatedAt = now
	j.LeaseUntil = nil
	r.jobs[id] = j
	return nil
}

func (r *repo) Heartbeat(ctx context.Context, id uuid.UUID, workerID string, leaseGen int, lease time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok {
		return orderedjob.ErrNotFound
	}
	if j.WorkerID != workerID || j.LeaseGeneration != leaseGen {
		return orderedjob.ErrLeaseConflict
	}
	now := time.Now().UTC()
	until := now.Add(lease)
	j.LeaseUntil = &until
	j.UpdatedAt = now
	r.jobs[id] = j
	return nil
}

func (r *repo) RequestCancel(ctx context.Context, id uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok {
		return orderedjob.ErrNotFound
	}
	if j.Status == lifecycle.StatePending || j.Status == lifecycle.StateRetrying || j.Status == lifecycle.StateBlocked {
		j.Status = lifecycle.StateCancelRequested
		j.UpdatedAt = time.Now().UTC()
		r.jobs[id] = j
	}
	return nil
}

func (r *repo) ConfirmCancel(ctx context.Context, id uuid.UUID, workerID string, leaseGen int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok {
		return orderedjob.ErrNotFound
	}
	if j.WorkerID != workerID || j.LeaseGeneration != leaseGen {
		return orderedjob.ErrLeaseConflict
	}
	j.Status = lifecycle.StateCancelled
	j.UpdatedAt = time.Now().UTC()
	j.LeaseUntil = nil
	r.jobs[id] = j
	return nil
}

func (r *repo) RecoverStale(ctx context.Context, limit int, retryPolicy func(attempt int) time.Duration) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now().UTC()
	count := 0
	for id, j := range r.jobs {
		if count >= limit {
			break
		}
		if j.Status == lifecycle.StateProcessing && j.LeaseUntil != nil && j.LeaseUntil.Before(now) {
			// stale
			delay := time.Second
			if retryPolicy != nil {
				delay = retryPolicy(j.Attempt)
			}
			j.Status = lifecycle.StateRetrying
			j.AvailableAt = now.Add(delay)
			j.LastError = "stale lease recovered"
			j.UpdatedAt = now
			j.LeaseUntil = nil
			r.jobs[id] = j
			count++
		}
	}
	return count, nil
}

func (r *repo) PromoteNext(ctx context.Context, chainID string, completedSeq int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.chain[chainID]; ok {
		if nextID, ok := m[completedSeq+1]; ok {
			next := r.jobs[nextID]
			if next.Status == lifecycle.StateBlocked {
				next.Status = lifecycle.StatePending
				next.UpdatedAt = time.Now().UTC()
				r.jobs[nextID] = next
			}
		}
	}
	return nil
}

func (r *repo) ListPendingChains(ctx context.Context) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	set := map[string]bool{}
	for _, j := range r.jobs {
		if j.Status == lifecycle.StatePending {
			set[j.ChainID] = true
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out, nil
}

func (r *repo) Listen(ctx context.Context, callback func(chainID string)) error {
	<-ctx.Done()
	return nil
}

func (r *repo) ReplayJob(ctx context.Context, id uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok {
		return orderedjob.ErrNotFound
	}
	if j.Status == lifecycle.StateFailed || j.Status == lifecycle.StateCancelled || j.Status == lifecycle.StateDeadLettered {
		now := time.Now().UTC()
		j.Status = lifecycle.StatePending
		j.Attempt = 0
		j.LastError = ""
		j.AvailableAt = now
		j.UpdatedAt = now
		j.LeaseUntil = nil
		r.jobs[id] = j
	}
	return nil
}

func (r *repo) ReplayJobWithPayload(ctx context.Context, id uuid.UUID, newPayload json.RawMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok {
		return orderedjob.ErrNotFound
	}
	if j.Status == lifecycle.StateFailed || j.Status == lifecycle.StateCancelled || j.Status == lifecycle.StateDeadLettered {
		now := time.Now().UTC()
		j.Status = lifecycle.StatePending
		j.Attempt = 0
		j.LastError = ""
		if len(newPayload) > 0 {
			j.Payload = newPayload
		}
		j.AvailableAt = now
		j.UpdatedAt = now
		j.LeaseUntil = nil
		r.jobs[id] = j
	}
	return nil
}

func (r *repo) SkipJob(ctx context.Context, id uuid.UUID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok {
		return orderedjob.ErrNotFound
	}
	if j.Status == lifecycle.StateFailed || j.Status == lifecycle.StateCancelled || j.Status == lifecycle.StateDeadLettered {
		now := time.Now().UTC()
		j.Status = lifecycle.StateCompleted
		j.CompletedAt = &now
		j.UpdatedAt = now
		j.LeaseUntil = nil
		r.jobs[id] = j

		if m, ok := r.chain[j.ChainID]; ok {
			if nextID, ok := m[j.Sequence+1]; ok {
				next := r.jobs[nextID]
				if next.Status == lifecycle.StateBlocked {
					next.Status = lifecycle.StatePending
					next.UpdatedAt = now
					r.jobs[nextID] = next
				}
			}
		}
	}
	return nil
}

func (r *repo) RescheduleJob(ctx context.Context, id uuid.UUID, availableAt time.Time) (orderedjob.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	j, exists := r.jobs[id]
	if !exists {
		return orderedjob.Job{}, orderedjob.ErrNotFound
	}

	if j.Status != lifecycle.StatePending && j.Status != lifecycle.StateRetrying {
		return orderedjob.Job{}, orderedjob.ErrJobNotEligible
	}

	now := time.Now().UTC()
	j.AvailableAt = availableAt
	j.UpdatedAt = now
	r.jobs[id] = j

	return j, nil
}

func (r *repo) GetStats(ctx context.Context) (orderedjob.Stats, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var stats orderedjob.Stats
	stats.Total = int64(len(r.jobs))
	stats.ActiveChains = int64(len(r.chain))
	now := time.Now().UTC()

	for _, j := range r.jobs {
		if (j.Status == lifecycle.StatePending || j.Status == lifecycle.StateRetrying) && j.AvailableAt.After(now) {
			stats.Scheduled++
		}
		switch j.Status {
		case lifecycle.StatePending:
			stats.Pending++
		case lifecycle.StateBlocked:
			stats.Blocked++
		case lifecycle.StateProcessing:
			stats.Processing++
		case lifecycle.StateCompleted:
			stats.Completed++
		case lifecycle.StateRetrying:
			stats.Retrying++
		case lifecycle.StateFailed:
			stats.Failed++
		case lifecycle.StateDeadLettered:
			stats.DeadLettered++
		case lifecycle.StateCancelRequested:
			stats.CancelRequested++
		case lifecycle.StateCancelled:
			stats.Cancelled++
		}
	}

	return stats, nil
}

func (r *repo) ListJobs(ctx context.Context, filter orderedjob.JobFilter) ([]orderedjob.Job, int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var matched []orderedjob.Job
	search := strings.ToLower(filter.Search)
	now := time.Now().UTC()

	for _, j := range r.jobs {
		if filter.ScheduledOnly {
			if (j.Status != lifecycle.StatePending && j.Status != lifecycle.StateRetrying) || !j.AvailableAt.After(now) {
				continue
			}
		}
		if filter.ChainID != "" && j.ChainID != filter.ChainID {
			continue
		}
		if filter.Status != "" && j.Status != filter.Status {
			continue
		}
		if filter.JobType != "" && j.Type != filter.JobType {
			continue
		}
		if filter.TenantID != "" && j.TenantID != filter.TenantID {
			continue
		}
		if filter.TraceID != "" && j.TraceID != filter.TraceID {
			continue
		}
		if search != "" {
			idStr := strings.ToLower(j.ID.String())
			keyStr := strings.ToLower(j.IdempotencyKey)
			traceStr := strings.ToLower(j.TraceID)
			if !strings.Contains(idStr, search) && !strings.Contains(keyStr, search) && !strings.Contains(traceStr, search) {
				continue
			}
		}
		matched = append(matched, j)
	}

	isAsc := strings.EqualFold(filter.OrderDir, "ASC")
	sort.Slice(matched, func(i, j int) bool {
		var less bool
		switch strings.ToLower(filter.OrderBy) {
		case "sequence":
			less = matched[i].Sequence < matched[j].Sequence
		case "job_type":
			less = matched[i].Type < matched[j].Type
		case "status":
			less = matched[i].Status < matched[j].Status
		case "available_at":
			less = matched[i].AvailableAt.Before(matched[j].AvailableAt)
		default: // created_at
			less = matched[i].CreatedAt.Before(matched[j].CreatedAt)
		}
		if isAsc {
			return less
		}
		return !less
	})

	total := int64(len(matched))
	if filter.Offset >= len(matched) {
		return []orderedjob.Job{}, total, nil
	}

	end := filter.Offset + filter.Limit
	if filter.Limit <= 0 || end > len(matched) {
		end = len(matched)
	}

	return matched[filter.Offset:end], total, nil
}

func (r *repo) ListChains(ctx context.Context, filter orderedjob.ChainFilter) ([]orderedjob.ChainSummary, int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	search := strings.ToLower(filter.Search)
	var summaries []orderedjob.ChainSummary

	for chainID, m := range r.chain {
		if search != "" && !strings.Contains(strings.ToLower(chainID), search) {
			continue
		}

		var cs orderedjob.ChainSummary
		cs.ChainID = chainID
		cs.TotalJobs = int64(len(m))

		var maxSeq int64
		var latestJob orderedjob.Job
		for seq, id := range m {
			if seq > maxSeq {
				maxSeq = seq
				latestJob = r.jobs[id]
			}
			j := r.jobs[id]
			if j.Status == lifecycle.StatePending || j.Status == lifecycle.StateProcessing || j.Status == lifecycle.StateRetrying {
				cs.PendingJobs++
			}
			if j.Status == lifecycle.StateFailed || j.Status == lifecycle.StateDeadLettered {
				cs.FailedJobs++
			}
		}
		cs.MaxSequence = maxSeq
		cs.LatestStatus = latestJob.Status
		summaries = append(summaries, cs)
	}

	isAsc := strings.EqualFold(filter.OrderDir, "ASC")
	if filter.OrderDir == "" {
		isAsc = true
	}

	sort.Slice(summaries, func(i, j int) bool {
		var less bool
		switch strings.ToLower(filter.OrderBy) {
		case "total_jobs":
			less = summaries[i].TotalJobs < summaries[j].TotalJobs
		case "max_sequence":
			less = summaries[i].MaxSequence < summaries[j].MaxSequence
		case "pending_jobs":
			less = summaries[i].PendingJobs < summaries[j].PendingJobs
		case "failed_jobs":
			less = summaries[i].FailedJobs < summaries[j].FailedJobs
		default: // chain_id
			less = summaries[i].ChainID < summaries[j].ChainID
		}
		if isAsc {
			return less
		}
		return !less
	})

	total := int64(len(summaries))
	if filter.Offset >= len(summaries) {
		return []orderedjob.ChainSummary{}, total, nil
	}

	end := filter.Offset + filter.Limit
	if filter.Limit <= 0 || end > len(summaries) {
		end = len(summaries)
	}

	return summaries[filter.Offset:end], total, nil
}
