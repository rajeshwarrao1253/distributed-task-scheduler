// Package store provides PostgreSQL-backed persistence for the distributed task scheduler.
// It handles job CRUD operations, status tracking, pagination, and audit history.
//
// All database operations use context for timeout/cancellation propagation and
// connection pooling via pgx for high concurrency.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/models"
)

// =============================================================================
// Errors
// =============================================================================

var (
	ErrNotFound     = errors.New("record not found")
	ErrConflict     = errors.New("record already exists")
	ErrInvalidInput = errors.New("invalid input")
)

// =============================================================================
// Store Interface
// =============================================================================

// Store defines the persistence interface for job management.
// This abstraction allows swapping PostgreSQL with other backends for testing.
type Store interface {
	// CreateJob persists a new job to the store.
	CreateJob(ctx context.Context, job *models.Job) error

	// GetJob retrieves a job by its ID.
	GetJob(ctx context.Context, id uuid.UUID) (*models.Job, error)

	// UpdateJobStatus atomically updates a job's status and related fields.
	UpdateJobStatus(ctx context.Context, id uuid.UUID, status models.JobStatus, opts ...UpdateOpt) error

	// DeleteJob permanently removes a job and its history.
	DeleteJob(ctx context.Context, id uuid.UUID) error

	// ListJobs returns jobs filtered by status with pagination.
	ListJobs(ctx context.Context, filter JobFilter) ([]*models.Job, int64, error)

	// GetJobHistory returns the audit trail for a job.
	GetJobHistory(ctx context.Context, jobID uuid.UUID, limit, offset int) ([]*models.JobHistory, error)

	// GetQueueStats returns aggregate statistics for monitoring.
	GetQueueStats(ctx context.Context) (*QueueStats, error)

	// RecordHeartbeat updates a worker's liveness timestamp.
	RecordHeartbeat(ctx context.Context, heartbeat *models.WorkerHeartbeat) error

	// GetActiveWorkers returns workers that have heartbeated recently.
	GetActiveWorkers(ctx context.Context, since time.Duration) ([]*models.WorkerHeartbeat, error)

	// PruneOldHeartbeats removes stale worker heartbeat records.
	PruneOldHeartbeats(ctx context.Context, maxAge time.Duration) (int64, error)

	// Close releases database connections.
	Close()
}

// =============================================================================
// UpdateOpt
// =============================================================================

// UpdateOpt is a functional option for UpdateJobStatus.
type UpdateOpt func(*updateOpts)

type updateOpts struct {
	workerID     string
	errorMessage string
	startedAt    *time.Time
	completedAt  *time.Time
}

// WithWorkerID sets the worker ID during status update.
func WithWorkerID(id string) UpdateOpt {
	return func(o *updateOpts) {
		o.workerID = id
	}
}

// WithErrorMessage sets the error message during status update.
func WithErrorMessage(msg string) UpdateOpt {
	return func(o *updateOpts) {
		o.errorMessage = msg
	}
}

// WithStartedAt sets the started timestamp.
func WithStartedAt(t time.Time) UpdateOpt {
	return func(o *updateOpts) {
		o.startedAt = &t
	}
}

// WithCompletedAt sets the completed timestamp.
func WithCompletedAt(t time.Time) UpdateOpt {
	return func(o *updateOpts) {
		o.completedAt = &t
	}
}

// =============================================================================
// JobFilter
// =============================================================================

// JobFilter provides filtering and pagination for job queries.
type JobFilter struct {
	Status     *models.JobStatus
	Type       string
	Priority   *int
	WorkerID   string
	Limit      int
	Offset     int
	OrderBy    string
	OrderDesc  bool
}

// =============================================================================
// QueueStats
// =============================================================================

// QueueStats provides aggregate queue metrics.
type QueueStats struct {
	Pending      int64
	Running      int64
	Completed    int64
	Failed       int64
	DeadLetter   int64
	Cancelled    int64
	Total        int64
	AvgDuration  *time.Duration
}

// =============================================================================
// PostgreSQLStore
// =============================================================================

// PostgreSQLStore implements the Store interface using PostgreSQL.
type PostgreSQLStore struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

// NewPostgreSQLStore creates a new PostgreSQL-backed store.
func NewPostgreSQLStore(ctx context.Context, dsn string, logger *zap.Logger) (*PostgreSQLStore, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	// Connection pool tuning for production
	config.MaxConns = 50
	config.MinConns = 10
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute
	config.HealthCheckPeriod = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	// Verify connectivity
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping database: %w", err)
	}

	s := &PostgreSQLStore{
		pool:   pool,
		logger: logger.With(zap.String("component", "postgres_store")),
	}

	s.logger.Info("postgresql store initialized")
	return s, nil
}

// ---------------------------------------------------------------------------
// CreateJob
// ---------------------------------------------------------------------------

// CreateJob persists a new job. The job ID is set by the caller.
func (s *PostgreSQLStore) CreateJob(ctx context.Context, job *models.Job) error {
	query := `
		INSERT INTO jobs (
			id, job_type, payload, priority, status, scheduled_at,
			retry_count, max_retries, timeout_ms, cron_expression, cron_next_run,
			created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	`

	var cronNextRun *time.Time
	if job.CronExpression != "" {
		cronNextRun = job.CronNextRun
	}

	_, err := s.pool.Exec(ctx, query,
		job.ID,
		job.Type,
		job.Payload,
		job.Priority,
		job.Status,
		job.ScheduledAt,
		job.RetryCount,
		job.MaxRetries,
		job.Timeout,
		job.CronExpression,
		cronNextRun,
		job.CreatedAt,
		job.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}

	s.logger.Debug("job created", zap.String("job_id", job.ID.String()))
	return nil
}

// ---------------------------------------------------------------------------
// GetJob
// ---------------------------------------------------------------------------

// GetJob retrieves a job by ID.
func (s *PostgreSQLStore) GetJob(ctx context.Context, id uuid.UUID) (*models.Job, error) {
	query := `
		SELECT 
			id, job_type, payload, priority, status, scheduled_at,
			retry_count, max_retries, timeout_ms, cron_expression, cron_next_run,
			started_at, completed_at, worker_id, error_message,
			created_at, updated_at
		FROM jobs WHERE id = $1
	`

	job := &models.Job{}
	err := s.pool.QueryRow(ctx, query, id).Scan(
		&job.ID,
		&job.Type,
		&job.Payload,
		&job.Priority,
		&job.Status,
		&job.ScheduledAt,
		&job.RetryCount,
		&job.MaxRetries,
		&job.Timeout,
		&job.CronExpression,
		&job.CronNextRun,
		&job.StartedAt,
		&job.CompletedAt,
		&job.WorkerID,
		&job.ErrorMessage,
		&job.CreatedAt,
		&job.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, models.ErrJobNotFound
		}
		return nil, fmt.Errorf("get job: %w", err)
	}

	return job, nil
}

// ---------------------------------------------------------------------------
// UpdateJobStatus
// ---------------------------------------------------------------------------

// UpdateJobStatus atomically updates a job's status with optional fields.
func (s *PostgreSQLStore) UpdateJobStatus(
	ctx context.Context,
	id uuid.UUID,
	status models.JobStatus,
	opts ...UpdateOpt,
) error {
	options := &updateOpts{}
	for _, opt := range opts {
		opt(options)
	}

	query := `
		UPDATE jobs SET
			status = $1,
			worker_id = COALESCE(NULLIF($2, ''), worker_id),
			error_message = COALESCE($3, error_message),
			started_at = COALESCE($4, started_at),
			completed_at = COALESCE($5, completed_at),
			updated_at = NOW()
		WHERE id = $6
	`

	result, err := s.pool.Exec(ctx, query,
		status,
		options.workerID,
		options.errorMessage,
		options.startedAt,
		options.completedAt,
		id,
	)
	if err != nil {
		return fmt.Errorf("update job status: %w", err)
	}

	if result.RowsAffected() == 0 {
		return models.ErrJobNotFound
	}

	return nil
}

// ---------------------------------------------------------------------------
// DeleteJob
// ---------------------------------------------------------------------------

// DeleteJob permanently removes a job. History records are cascade-deleted.
func (s *PostgreSQLStore) DeleteJob(ctx context.Context, id uuid.UUID) error {
	query := `DELETE FROM jobs WHERE id = $1`

	result, err := s.pool.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("delete job: %w", err)
	}

	if result.RowsAffected() == 0 {
		return models.ErrJobNotFound
	}

	s.logger.Debug("job deleted", zap.String("job_id", id.String()))
	return nil
}

// ---------------------------------------------------------------------------
// ListJobs
// ---------------------------------------------------------------------------

// ListJobs returns jobs matching the filter with pagination.
func (s *PostgreSQLStore) ListJobs(
	ctx context.Context,
	filter JobFilter,
) ([]*models.Job, int64, error) {
	// Default pagination
	if filter.Limit <= 0 || filter.Limit > 1000 {
		filter.Limit = 100
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}
	if filter.OrderBy == "" {
		filter.OrderBy = "created_at"
	}
	orderDir := "ASC"
	if filter.OrderDesc {
		orderDir = "DESC"
	}

	// Build query dynamically
	whereClause := "WHERE 1=1"
	args := []interface{}{}
	argIdx := 1

	if filter.Status != nil {
		whereClause += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, *filter.Status)
		argIdx++
	}
	if filter.Type != "" {
		whereClause += fmt.Sprintf(" AND job_type = $%d", argIdx)
		args = append(args, filter.Type)
		argIdx++
	}
	if filter.Priority != nil {
		whereClause += fmt.Sprintf(" AND priority = $%d", argIdx)
		args = append(args, *filter.Priority)
		argIdx++
	}
	if filter.WorkerID != "" {
		whereClause += fmt.Sprintf(" AND worker_id = $%d", argIdx)
		args = append(args, filter.WorkerID)
		argIdx++
	}

	// Get total count
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM jobs %s", whereClause)
	var total int64
	if err := s.pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count jobs: %w", err)
	}

	// Get paginated results
	query := fmt.Sprintf(`
		SELECT 
			id, job_type, payload, priority, status, scheduled_at,
			retry_count, max_retries, timeout_ms, cron_expression, cron_next_run,
			started_at, completed_at, worker_id, error_message,
			created_at, updated_at
		FROM jobs
		%s
		ORDER BY %s %s
		LIMIT $%d OFFSET $%d
	`, whereClause, filter.OrderBy, orderDir, argIdx, argIdx+1)

	args = append(args, filter.Limit, filter.Offset)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	jobs, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[models.Job])
	if err != nil {
		return nil, 0, fmt.Errorf("scan jobs: %w", err)
	}

	return jobs, total, nil
}

// ---------------------------------------------------------------------------
// GetJobHistory
// ---------------------------------------------------------------------------

// GetJobHistory returns the audit trail for a job.
func (s *PostgreSQLStore) GetJobHistory(
	ctx context.Context,
	jobID uuid.UUID,
	limit, offset int,
) ([]*models.JobHistory, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	query := `
		SELECT id, job_id, old_status, new_status, worker_id, 
		       error_message, metadata, created_at
		FROM job_history
		WHERE job_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := s.pool.Query(ctx, query, jobID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("get job history: %w", err)
	}
	defer rows.Close()

	var history []*models.JobHistory
	for rows.Next() {
		h := &models.JobHistory{}
		var oldStatus *string
		err := rows.Scan(
			&h.ID, &h.JobID, &oldStatus, &h.NewStatus,
			&h.WorkerID, &h.ErrorMessage, &h.Metadata, &h.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan history: %w", err)
		}
		if oldStatus != nil {
			s := models.JobStatus(*oldStatus)
			h.OldStatus = &s
		}
		history = append(history, h)
	}

	return history, rows.Err()
}

// ---------------------------------------------------------------------------
// GetQueueStats
// ---------------------------------------------------------------------------

// GetQueueStats returns aggregate statistics for all job statuses.
func (s *PostgreSQLStore) GetQueueStats(ctx context.Context) (*QueueStats, error) {
	query := `
		SELECT 
			COUNT(*) FILTER (WHERE status = 'pending') as pending,
			COUNT(*) FILTER (WHERE status = 'running') as running,
			COUNT(*) FILTER (WHERE status = 'completed') as completed,
			COUNT(*) FILTER (WHERE status = 'failed') as failed,
			COUNT(*) FILTER (WHERE status = 'dead_letter') as dead_letter,
			COUNT(*) FILTER (WHERE status = 'cancelled') as cancelled,
			COUNT(*) as total,
			AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) * 1000 as avg_duration_ms
		FROM jobs
	`

	stats := &QueueStats{}
	var avgDurationMs *float64
	err := s.pool.QueryRow(ctx, query).Scan(
		&stats.Pending,
		&stats.Running,
		&stats.Completed,
		&stats.Failed,
		&stats.DeadLetter,
		&stats.Cancelled,
		&stats.Total,
		&avgDurationMs,
	)
	if err != nil {
		return nil, fmt.Errorf("get queue stats: %w", err)
	}

	if avgDurationMs != nil {
		d := time.Duration(*avgDurationMs) * time.Millisecond
		stats.AvgDuration = &d
	}

	return stats, nil
}

// ---------------------------------------------------------------------------
// Heartbeat Operations
// ---------------------------------------------------------------------------

// RecordHeartbeat upserts a worker's heartbeat record.
func (s *PostgreSQLStore) RecordHeartbeat(ctx context.Context, hb *models.WorkerHeartbeat) error {
	query := `
		INSERT INTO worker_heartbeats (worker_id, hostname, concurrency, active_jobs, last_heartbeat, started_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (worker_id) DO UPDATE SET
			hostname = EXCLUDED.hostname,
			concurrency = EXCLUDED.concurrency,
			active_jobs = EXCLUDED.active_jobs,
			last_heartbeat = EXCLUDED.last_heartbeat
	`

	_, err := s.pool.Exec(ctx, query,
		hb.WorkerID, hb.Hostname, hb.Concurrency, hb.ActiveJobs,
		hb.LastHeartbeat, hb.StartedAt,
	)
	if err != nil {
		return fmt.Errorf("record heartbeat: %w", err)
	}

	return nil
}

// GetActiveWorkers returns workers that have heartbeated within the given duration.
func (s *PostgreSQLStore) GetActiveWorkers(ctx context.Context, since time.Duration) ([]*models.WorkerHeartbeat, error) {
	query := `
		SELECT worker_id, hostname, concurrency, active_jobs, last_heartbeat, started_at
		FROM worker_heartbeats
		WHERE last_heartbeat > $1
		ORDER BY last_heartbeat DESC
	`

	cutoff := time.Now().UTC().Add(-since)
	rows, err := s.pool.Query(ctx, query, cutoff)
	if err != nil {
		return nil, fmt.Errorf("get active workers: %w", err)
	}
	defer rows.Close()

	var workers []*models.WorkerHeartbeat
	for rows.Next() {
		w := &models.WorkerHeartbeat{}
		err := rows.Scan(&w.WorkerID, &w.Hostname, &w.Concurrency, &w.ActiveJobs, &w.LastHeartbeat, &w.StartedAt)
		if err != nil {
			return nil, fmt.Errorf("scan worker: %w", err)
		}
		workers = append(workers, w)
	}

	return workers, rows.Err()
}

// PruneOldHeartbeats removes worker records older than maxAge.
func (s *PostgreSQLStore) PruneOldHeartbeats(ctx context.Context, maxAge time.Duration) (int64, error) {
	query := `DELETE FROM worker_heartbeats WHERE last_heartbeat < $1`
	cutoff := time.Now().UTC().Add(-maxAge)

	result, err := s.pool.Exec(ctx, query, cutoff)
	if err != nil {
		return 0, fmt.Errorf("prune heartbeats: %w", err)
	}

	return result.RowsAffected(), nil
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

// Close closes the database connection pool.
func (s *PostgreSQLStore) Close() {
	s.pool.Close()
	s.logger.Info("postgresql store closed")
}