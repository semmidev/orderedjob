package ui

import (
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/url"
	"strconv"
	"strings"

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

	filter := orderedjob.JobFilter{
		ChainID:  chainID,
		Status:   status,
		JobType:  jobType,
		Search:   search,
		OrderBy:  orderBy,
		OrderDir: orderDir,
		Offset:   (page - 1) * limit,
		Limit:    limit,
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
		err = s.repo.ReplayJob(r.Context(), id)
	case "skip":
		err = s.repo.SkipJob(r.Context(), id)
	case "cancel":
		err = s.repo.RequestCancel(r.Context(), id)
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
