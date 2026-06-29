// Package scheduler provides the HTTP REST API for job management.
// All endpoints return JSON and use standard HTTP status codes.
//
// Endpoints:
//   POST /jobs        - Submit a new job
//   GET  /jobs/:id    - Get job details
//   GET  /jobs/:id/status - Get job status
//   DELETE /jobs/:id  - Delete a job
//   POST /jobs/:id/retry - Retry a failed job
//   GET  /jobs        - List jobs (with query params for filtering)
//   GET  /health      - Health check
//   GET  /metrics     - Prometheus metrics
package scheduler

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/metrics"
	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/models"
	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/store"
)

// =============================================================================
// Server
// =============================================================================

// Server is the HTTP API server for the scheduler.
type Server struct {
	core    *Core
	metrics *metrics.Collector
	logger  *zap.Logger
	router  *chi.Mux
	addr    string
}

// NewServer creates a new HTTP API server.
func NewServer(core *Core, m *metrics.Collector, addr string, logger *zap.Logger) *Server {
	s := &Server{
		core:    core,
		metrics: m,
		logger:  logger.With(zap.String("component", "api_server")),
		addr:    addr,
	}

	s.router = s.setupRoutes()
	return s
}

// setupRoutes configures the HTTP router.
func (s *Server) setupRoutes() *chi.Mux {
	r := chi.NewRouter()

	// Middleware
	r.Use(s.loggingMiddleware)
	r.Use(s.recoveryMiddleware)
	r.Use(render.SetContentType(render.ContentTypeJSON))

	// Job routes
	r.Route("/jobs", func(r chi.Router) {
		r.Post("/", s.handleSubmitJob)
		r.Get("/", s.handleListJobs)
		r.Route("/{id}", func(r chi.Router) {
			r.Get("/", s.handleGetJob)
			r.Get("/status", s.handleGetJobStatus)
			r.Delete("/", s.handleDeleteJob)
			r.Post("/retry", s.handleRetryJob)
		})
	})

	// Health
	r.Get("/health", s.handleHealth)

	// Metrics
	r.Get("/metrics", func(w http.ResponseWriter, r *http.Request) {
		s.metrics.HTTPHandler().ServeHTTP(w, r)
	})

	return r
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

// Start begins listening for HTTP requests.
func (s *Server) Start() error {
	s.logger.Info("starting HTTP server", zap.String("addr", s.addr))
	return http.ListenAndServe(s.addr, s.router)
}

// =============================================================================
// Middleware
// =============================================================================

// loggingMiddleware logs all HTTP requests.
func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap response writer to capture status code
		ww := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(ww, r)

		s.logger.Debug("http request",
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.Int("status", ww.statusCode),
			zap.Duration("duration", time.Since(start)),
			zap.String("remote_addr", r.RemoteAddr),
		)
	})
}

// recoveryMiddleware recovers from panics in handlers.
func (s *Server) recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.logger.Error("handler panic",
					zap.Any("recover", rec),
					zap.String("path", r.URL.Path),
				)
				s.renderError(w, r, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// responseWriter wraps http.ResponseWriter to capture status code.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *responseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// =============================================================================
// Handlers
// =============================================================================

// handleSubmitJob handles POST /jobs - create a new job.
func (s *Server) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	var req models.SubmitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.renderError(w, r, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if err := req.Validate(); err != nil {
		s.renderError(w, r, http.StatusBadRequest, err.Error())
		return
	}

	var job *models.Job
	var err error

	if req.Cron != "" {
		// Cron job registration
		job, err = s.core.RegisterCronJob(r.Context(), &req)
	} else {
		// Regular job
		job, err = s.core.SubmitJob(r.Context(), &req)
	}

	if err != nil {
		s.logger.Error("failed to submit job", zap.Error(err))
		s.renderError(w, r, http.StatusInternalServerError, "failed to submit job: "+err.Error())
		return
	}

	s.renderJSON(w, r, http.StatusCreated, models.JobResponse{
		ID:        job.ID,
		Status:    job.Status,
		CreatedAt: job.CreatedAt,
	})
}

// handleGetJob handles GET /jobs/:id - get job details.
func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "invalid job id")
		return
	}

	job, err := s.core.GetJob(r.Context(), id)
	if err != nil {
		if err == models.ErrJobNotFound {
			s.renderError(w, r, http.StatusNotFound, "job not found")
			return
		}
		s.renderError(w, r, http.StatusInternalServerError, "failed to get job: "+err.Error())
		return
	}

	s.renderJSON(w, r, http.StatusOK, job)
}

// handleGetJobStatus handles GET /jobs/:id/status - get job status.
func (s *Server) handleGetJobStatus(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "invalid job id")
		return
	}

	status, err := s.core.GetJobStatus(r.Context(), id)
	if err != nil {
		if err == models.ErrJobNotFound {
			s.renderError(w, r, http.StatusNotFound, "job not found")
			return
		}
		s.renderError(w, r, http.StatusInternalServerError, "failed to get job status: "+err.Error())
		return
	}

	s.renderJSON(w, r, http.StatusOK, status)
}

// handleDeleteJob handles DELETE /jobs/:id - delete a job.
func (s *Server) handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "invalid job id")
		return
	}

	if err := s.core.DeleteJob(r.Context(), id); err != nil {
		if err == models.ErrJobNotFound {
			s.renderError(w, r, http.StatusNotFound, "job not found")
			return
		}
		s.renderError(w, r, http.StatusInternalServerError, "failed to delete job: "+err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleRetryJob handles POST /jobs/:id/retry - retry a failed job.
func (s *Server) handleRetryJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "invalid job id")
		return
	}

	if err := s.core.RetryJob(r.Context(), id); err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "failed to retry job: "+err.Error())
		return
	}

	s.renderJSON(w, r, http.StatusOK, map[string]string{"status": "retry queued"})
}

// handleListJobs handles GET /jobs - list jobs with filtering.
func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	filter := store.JobFilter{
		Limit:  100,
		Offset: 0,
	}

	// Parse query parameters
	if statusStr := r.URL.Query().Get("status"); statusStr != "" {
		status := models.JobStatus(statusStr)
		filter.Status = &status
	}
	if jobType := r.URL.Query().Get("type"); jobType != "" {
		filter.Type = jobType
	}
	if priorityStr := r.URL.Query().Get("priority"); priorityStr != "" {
		if p, err := strconv.Atoi(priorityStr); err == nil && p >= 0 && p <= 9 {
			filter.Priority = &p
		}
	}
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
			filter.Limit = l
		}
	}
	if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
			filter.Offset = o
		}
	}
	if workerID := r.URL.Query().Get("worker_id"); workerID != "" {
		filter.WorkerID = workerID
	}
	filter.OrderDesc = r.URL.Query().Get("order") == "desc"

	jobs, total, err := s.core.ListJobs(r.Context(), filter)
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "failed to list jobs: "+err.Error())
		return
	}

	s.renderJSON(w, r, http.StatusOK, map[string]interface{}{
		"jobs":  jobs,
		"total": total,
		"limit": filter.Limit,
		"offset": filter.Offset,
	})
}

// handleHealth handles GET /health - health check.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.renderJSON(w, r, http.StatusOK, map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().UTC(),
	})
}

// =============================================================================
// Response Helpers
// =============================================================================

// renderJSON renders a JSON response.
func (s *Server) renderJSON(w http.ResponseWriter, r *http.Request, status int, v interface{}) {
	render.Status(r, status)
	render.JSON(w, r, v)
}

// renderError renders an error response.
func (s *Server) renderError(w http.ResponseWriter, r *http.Request, status int, message string) {
	s.renderJSON(w, r, status, map[string]string{
		"error": message,
	})
}

// =============================================================================
// Request ID Middleware
// =============================================================================

// requestIDKey is the context key for request IDs.
type requestIDKey struct{}

// RequestIDMiddleware adds a request ID to each request for tracing.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = uuid.New().String()
		}
		ctx := r.Context()
		ctx = context.WithValue(ctx, requestIDKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// =============================================================================
// CORS Middleware
// =============================================================================

// CORS adds Cross-Origin Resource Sharing headers.
func CORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}