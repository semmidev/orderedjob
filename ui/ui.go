package ui

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/semmidev/orderedjob"
)

//go:embed assets/*
var assetsFS embed.FS

// JobTypesProvider is implemented by anything that can return the registered
// job type names (e.g. *orderedjob.Engine).
type JobTypesProvider interface {
	JobTypes() []string
}

type Options struct {
	RootPath         string
	Title            string
	ReadOnly         bool
	JobTypesProvider JobTypesProvider
}

type Option func(*Options)

func WithRootPath(path string) Option {
	return func(o *Options) { o.RootPath = path }
}

func WithTitle(title string) Option {
	return func(o *Options) { o.Title = title }
}

func WithReadOnly(readOnly bool) Option {
	return func(o *Options) { o.ReadOnly = readOnly }
}

// WithJobTypesProvider wires an engine (or any JobTypesProvider) into the UI
// so the /api/job-types endpoint can return the registered handler names.
func WithJobTypesProvider(p JobTypesProvider) Option {
	return func(o *Options) { o.JobTypesProvider = p }
}

type Server struct {
	repo orderedjob.Repository
	opts Options
}

// NewHandler creates a standalone http.Handler serving the monitoring dashboard and REST APIs.
func NewHandler(repo orderedjob.Repository, opts ...Option) http.Handler {
	options := Options{
		RootPath: "/ui",
		Title:    "OrderedJob Dashboard",
		ReadOnly: false,
	}
	for _, o := range opts {
		o(&options)
	}

	options.RootPath = strings.TrimRight(options.RootPath, "/")
	if options.RootPath == "" {
		options.RootPath = "/ui"
	}

	s := &Server{
		repo: repo,
		opts: options,
	}

	subFS, err := fs.Sub(assetsFS, "assets")
	if err != nil {
		panic(err)
	}

	fileServer := http.FileServer(http.FS(subFS))

	mux := http.NewServeMux()

	mux.HandleFunc("/api/stats", s.handleStats)
	mux.HandleFunc("/api/jobs", s.handleJobs)
	mux.HandleFunc("/api/jobs/", s.handleJobDetailOrAction)
	mux.HandleFunc("/api/chains", s.handleChains)
	mux.HandleFunc("/api/enqueue", s.handleEnqueue)
	mux.HandleFunc("/api/job-types", s.handleJobTypes)
	mux.HandleFunc("/api/events", s.handleSSEEvents)
	mux.HandleFunc("/api/dlq/bulk-replay", s.handleBulkReplayDLQ)
	mux.HandleFunc("/api/dlq/bulk-skip", s.handleBulkSkipDLQ)
	mux.HandleFunc("/api/dlq/bulk-purge", s.handleBulkPurgeDLQ)

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if p == "" || p == "/" || p == "/index.html" || !strings.Contains(p, ".") {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			b, err := fs.ReadFile(subFS, "index.html")
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			htmlStr := string(b)
			htmlStr = strings.ReplaceAll(htmlStr, "{{ROOT_PATH}}", options.RootPath)
			htmlStr = strings.ReplaceAll(htmlStr, "{{TITLE}}", options.Title)
			htmlStr = strings.ReplaceAll(htmlStr, "{{READ_ONLY}}", strconv.FormatBool(options.ReadOnly))
			_, _ = w.Write([]byte(htmlStr))
			return
		}
		fileServer.ServeHTTP(w, r)
	})

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, options.RootPath) {
			r2 := new(http.Request)
			*r2 = *r
			r2.URL = new(url.URL)
			*r2.URL = *r.URL
			r2.URL.Path = strings.TrimPrefix(r.URL.Path, options.RootPath)
			if r2.URL.Path == "" {
				r2.URL.Path = "/"
			}
			mux.ServeHTTP(w, r2)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *Server) handleSSEEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	stats, err := s.repo.GetStats(r.Context())
	if err == nil {
		b, _ := json.Marshal(stats)
		_, _ = fmt.Fprintf(w, "event: stats\ndata: %s\n\n", string(b))
		flusher.Flush()
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			stats, err := s.repo.GetStats(r.Context())
			if err != nil {
				continue
			}
			b, _ := json.Marshal(stats)
			_, _ = fmt.Fprintf(w, "event: stats\ndata: %s\n\n", string(b))
			flusher.Flush()
		}
	}
}

func (s *Server) handleJobTypes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.opts.JobTypesProvider == nil {
		respondJSON(w, http.StatusOK, []string{})
		return
	}
	respondJSON(w, http.StatusOK, s.opts.JobTypesProvider.JobTypes())
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	stats, err := s.repo.GetStats(r.Context())
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respondJSON(w, http.StatusOK, stats)
}

func (s *Server) handleJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	chainID := q.Get("chain_id")
	status := q.Get("status")
	jobType := q.Get("job_type")
	tenantID := q.Get("tenant_id")
	traceID := q.Get("trace_id")
	search := q.Get("search")
	orderBy := q.Get("order_by")
	orderDir := q.Get("order_dir")

	page, _ := strconv.Atoi(q.Get("page"))
	if page < 1 {
		page = 1
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit < 1 || limit > 200 {
		limit = 20
	}

	scheduledOnly := q.Get("scheduled_only") == "true" || status == "SCHEDULED"
	if status == "SCHEDULED" {
		status = ""
	}

	filter := orderedjob.JobFilter{
		ChainID:       chainID,
		Status:        status,
		JobType:       jobType,
		TenantID:      tenantID,
		TraceID:       traceID,
		Search:        search,
		OrderBy:       orderBy,
		OrderDir:      orderDir,
		Offset:        (page - 1) * limit,
		Limit:         limit,
		ScheduledOnly: scheduledOnly,
	}

	jobs, total, err := s.repo.ListJobs(r.Context(), filter)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	totalPages := (total + int64(limit) - 1) / int64(limit)
	if totalPages < 1 {
		totalPages = 1
	}

	respondJSON(w, http.StatusOK, map[string]any{
		"jobs":        jobs,
		"total":       total,
		"page":        page,
		"limit":       limit,
		"total_pages": totalPages,
	})
}

func (s *Server) handleJobDetailOrAction(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/jobs/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "Job ID required", http.StatusBadRequest)
		return
	}

	id, err := uuid.Parse(parts[0])
	if err != nil {
		http.Error(w, "Invalid Job UUID", http.StatusBadRequest)
		return
	}

	// GET /api/jobs/{id}
	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		job, err := s.repo.Get(r.Context(), id)
		if err != nil {
			respondJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		respondJSON(w, http.StatusOK, job)
		return
	}

	// POST actions
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.opts.ReadOnly {
		respondJSON(w, http.StatusForbidden, map[string]string{"error": "Dashboard is in read-only mode"})
		return
	}

	action := parts[1]
	switch action {
	case "replay":
		var body struct {
			Payload json.RawMessage `json:"payload"`
		}
		if json.NewDecoder(r.Body).Decode(&body) == nil && len(body.Payload) > 0 {
			err = s.repo.ReplayJobWithPayload(r.Context(), id, body.Payload)
		} else {
			err = s.repo.ReplayJob(r.Context(), id)
		}
	case "skip":
		err = s.repo.SkipJob(r.Context(), id)
	case "cancel":
		err = s.repo.RequestCancel(r.Context(), id)
	case "reschedule":
		var body struct {
			AvailableAt time.Time `json:"available_at"`
		}
		if decodeErr := json.NewDecoder(r.Body).Decode(&body); decodeErr != nil || body.AvailableAt.IsZero() {
			respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid or missing available_at timestamp"})
			return
		}
		job, rescheduleErr := s.repo.RescheduleJob(r.Context(), id, body.AvailableAt)
		if rescheduleErr != nil {
			respondJSON(w, http.StatusInternalServerError, map[string]string{"error": rescheduleErr.Error()})
			return
		}
		respondJSON(w, http.StatusOK, map[string]any{"success": true, "id": id, "action": action, "job": job})
		return
	default:
		http.Error(w, "Unknown Action", http.StatusBadRequest)
		return
	}

	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{"success": true, "id": id, "action": action})
}

func (s *Server) handleChains(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	search := q.Get("search")
	orderBy := q.Get("order_by")
	orderDir := q.Get("order_dir")

	page, _ := strconv.Atoi(q.Get("page"))
	if page < 1 {
		page = 1
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit < 1 || limit > 200 {
		limit = 20
	}

	filter := orderedjob.ChainFilter{
		Search:   search,
		OrderBy:  orderBy,
		OrderDir: orderDir,
		Offset:   (page - 1) * limit,
		Limit:    limit,
	}

	chains, total, err := s.repo.ListChains(r.Context(), filter)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	totalPages := (total + int64(limit) - 1) / int64(limit)
	if totalPages < 1 {
		totalPages = 1
	}

	respondJSON(w, http.StatusOK, map[string]any{
		"chains":      chains,
		"total":       total,
		"page":        page,
		"limit":       limit,
		"total_pages": totalPages,
	})
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.opts.ReadOnly {
		respondJSON(w, http.StatusForbidden, map[string]string{"error": "Dashboard is in read-only mode"})
		return
	}
	var req orderedjob.EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request body: " + err.Error()})
		return
	}
	job, err := s.repo.Enqueue(r.Context(), req)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respondJSON(w, http.StatusOK, job)
}

func respondJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func (s *Server) parseJobFilterFromBody(r *http.Request) orderedjob.JobFilter {
	var body struct {
		ChainID  string `json:"chain_id"`
		JobType  string `json:"job_type"`
		TenantID string `json:"tenant_id"`
		TraceID  string `json:"trace_id"`
		Search   string `json:"search"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	return orderedjob.JobFilter{
		ChainID:  body.ChainID,
		JobType:  body.JobType,
		TenantID: body.TenantID,
		TraceID:  body.TraceID,
		Search:   body.Search,
	}
}

func (s *Server) handleBulkReplayDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.opts.ReadOnly {
		respondJSON(w, http.StatusForbidden, map[string]string{"error": "Dashboard is in read-only mode"})
		return
	}
	filter := s.parseJobFilterFromBody(r)
	affected, err := s.repo.BulkReplayDLQ(r.Context(), filter)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{"affected": affected, "message": fmt.Sprintf("%d DLQ jobs replayed successfully", affected)})
}

func (s *Server) handleBulkSkipDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.opts.ReadOnly {
		respondJSON(w, http.StatusForbidden, map[string]string{"error": "Dashboard is in read-only mode"})
		return
	}
	filter := s.parseJobFilterFromBody(r)
	affected, err := s.repo.BulkSkipDLQ(r.Context(), filter)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{"affected": affected, "message": fmt.Sprintf("%d DLQ jobs skipped successfully", affected)})
}

func (s *Server) handleBulkPurgeDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.opts.ReadOnly {
		respondJSON(w, http.StatusForbidden, map[string]string{"error": "Dashboard is in read-only mode"})
		return
	}
	filter := s.parseJobFilterFromBody(r)
	affected, err := s.repo.BulkPurgeDLQ(r.Context(), filter)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	respondJSON(w, http.StatusOK, map[string]any{"affected": affected, "message": fmt.Sprintf("%d DLQ jobs purged successfully", affected)})
}
