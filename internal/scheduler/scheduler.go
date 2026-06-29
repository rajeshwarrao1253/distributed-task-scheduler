// Package scheduler provides the core scheduling logic for the distributed task scheduler.
// It manages job prioritization, cron scheduling, delayed job processing, and
// coordination between the HTTP API and worker pools.
//
// The scheduler follows these design principles:
//   - Consensus-less: Uses Redis locks instead of consensus protocols
//   - At-least-once: Jobs are persisted before acknowledgement
//   - Priority-aware: Higher priority jobs (lower number) are dequeued first
//   - Observable: All operations emit metrics and structured logs
package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/lock"
	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/metrics"
	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/models"
	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/queue"
	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/store"
)

// =============================================================================
// Cron Expression Parser (simplified subset)
// =============================================================================

// CronSchedule represents a parsed cron expression for recurring jobs.
// Supports: standard 5-field cron (min hour dom month dow)
type CronSchedule struct {
	Expression string
	NextRun    time.Time
}

// ParseCron parses a cron expression and returns the next run time after `from`.
// This is a simplified implementation supporting basic cron patterns.
func ParseCron(expr string, from time.Time) (time.Time, error) {
	parts := strings.Fields(expr)
	if len(parts) != 5 {
		return time.Time{}, fmt.Errorf("invalid cron expression: expected 5 fields, got %d", len(parts))
	}

	minuteStr, hourStr, domStr, monthStr, dowStr := parts[0], parts[1], parts[2], parts[3], parts[4]

	// Start from the next minute boundary
	next := from.Truncate(time.Minute).Add(time.Minute)
	maxIterations := 366 * 24 * 60 // Up to ~1 year of minutes

	for i := 0; i < maxIterations; i++ {
		if matchCronField(minuteStr, next.Minute(), 0, 59) &&
			matchCronField(hourStr, next.Hour(), 0, 23) &&
			matchCronField(domStr, next.Day(), 1, 31) &&
			matchCronField(monthStr, int(next.Month()), 1, 12) &&
			matchCronField(dowStr, int(next.Weekday()), 0, 6) {
			return next, nil
		}
		next = next.Add(time.Minute)
	}

	return time.Time{}, fmt.Errorf("could not find next run time for expression: %s", expr)
}

// matchCronField checks if a value matches a cron field expression.
func matchCronField(field string, value, min, max int) bool {
	// Wildcard
	if field == "*" {
		return true
	}

	// Step expression (e.g., */5)
	if strings.HasPrefix(field, "*/") {
		step, err := strconv.Atoi(field[2:])
		if err != nil {
			return false
		}
		return value%step == 0
	}

	// Range (e.g., 1-5)
	if strings.Contains(field, "-") {
		parts := strings.Split(field, "-")
		if len(parts) == 2 {
			start, err1 := strconv.Atoi(parts[0])
			end, err2 := strconv.Atoi(parts[1])
			if err1 == nil && err2 == nil {
				return value >= start && value <= end
			}
		}
	}

	// List (e.g., 1,3,5)
	if strings.Contains(field, ",") {
		parts := strings.Split(field, ",")
		for _, p := range parts {
			v, err := strconv.Atoi(strings.TrimSpace(p))
			if err == nil && v == value {
				return true
			}
		}
		return false
	}

	// Single value
	v, err := strconv.Atoi(field)
	if err != nil {
		return false
	}
	return v == value
}

// =============================================================================
// Core Scheduler
// =============================================================================

// Core manages job scheduling logic: priority queues, cron execution,
// delayed job processing, and orphaned job reclamation.
type Core struct {
	queue   queue.Queue
	store   store.Store
	locker  lock.Locker
	metrics *metrics.Collector
	logger  *zap.Logger

	// Cron management
	cronMu      sync.RWMutex
	cronJobs    map[string]*models.Job
	cronStop    chan struct{}
	cronWg      sync.WaitGroup

	// Control
	stopChan chan struct{}
	stopOnce sync.Once
}

// NewCore creates a new scheduler core.
func NewCore(
	q queue.Queue,
	s store.Store,
	locker lock.Locker,
	m *metrics.Collector,
	logger *zap.Logger,
) *Core {
	return &Core{
		queue:    q,
		store:    s,
		locker:   locker,
		metrics:  m,
		logger:   logger.With(zap.String("component", "scheduler_core")),
		cronJobs: make(map[string]*models.Job),
		cronStop: make(chan struct{}),
		stopChan: make(chan struct{}),
	}
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

// Start begins the scheduler background loops.
func (c *Core) Start(ctx context.Context) error {
	c.logger.Info("starting scheduler core")

	// Start delayed job processor
	c.cronWg.Add(1)
	go c.delayedJobLoop(ctx)

	// Start orphaned job reclaimer
	c.cronWg.Add(1)
	go c.reclaimLoop(ctx)

	// Start cron job scheduler
	c.cronWg.Add(1)
	go c.cronLoop(ctx)

	<-c.stopChan
	c.logger.Info("scheduler core shutting down")

	close(c.cronStop)
	c.cronWg.Wait()

	return nil
}

// Shutdown gracefully stops the scheduler core.
func (c *Core) Shutdown() {
	c.stopOnce.Do(func() {
		close(c.stopChan)
	})
}

// ---------------------------------------------------------------------------
// SubmitJob
// ---------------------------------------------------------------------------

// SubmitJob creates and enqueues a new job.
func (c *Core) SubmitJob(ctx context.Context, req *models.SubmitJobRequest) (*models.Job, error) {
	start := time.Now()

	opts := []models.JobOption{
		models.WithPriority(req.Priority),
		models.WithMaxRetries(req.MaxRetries),
		models.WithTimeout(req.Timeout),
	}

	if req.ScheduledAt != nil {
		opts = append(opts, models.WithScheduledAt(*req.ScheduledAt))
	}

	if req.Cron != "" {
		opts = append(opts, models.WithCron(req.Cron))
	}

	job, err := models.NewJob(req.Type, req.Payload, opts...)
	if err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}

	// Calculate next cron run if applicable
	if req.Cron != "" {
		next, err := ParseCron(req.Cron, time.Now().UTC())
		if err != nil {
			return nil, fmt.Errorf("parse cron: %w", err)
		}
		job.CronNextRun = &next
	}

	// Persist to database first (durability)
	if err := c.store.CreateJob(ctx, job); err != nil {
		return nil, fmt.Errorf("persist job: %w", err)
	}

	// Enqueue to Redis
	if err := c.queue.Enqueue(ctx, job); err != nil {
		c.logger.Error("failed to enqueue job, will be picked up by recovery",
			zap.String("job_id", job.ID.String()),
			zap.Error(err),
		)
		// Job is in DB, recovery loop will pick it up
	}

	c.metrics.RecordJobSubmitted(job.Type, job.Priority)
	c.metrics.RecordEnqueueLatency(time.Since(start))

	c.logger.Info("job submitted",
		zap.String("job_id", job.ID.String()),
		zap.String("type", job.Type),
		zap.Int("priority", job.Priority),
	)

	return job, nil
}

// ---------------------------------------------------------------------------
// GetJob
// ---------------------------------------------------------------------------

// GetJob retrieves a job by ID from the store.
func (c *Core) GetJob(ctx context.Context, id uuid.UUID) (*models.Job, error) {
	return c.store.GetJob(ctx, id)
}

// ---------------------------------------------------------------------------
// GetJobStatus
// ---------------------------------------------------------------------------

// GetJobStatus returns the status of a job.
func (c *Core) GetJobStatus(ctx context.Context, id uuid.UUID) (*models.JobStatusResponse, error) {
	job, err := c.store.GetJob(ctx, id)
	if err != nil {
		return nil, err
	}

	return &models.JobStatusResponse{
		ID:           job.ID,
		Type:         job.Type,
		Status:       job.Status,
		Priority:     job.Priority,
		RetryCount:   job.RetryCount,
		MaxRetries:   job.MaxRetries,
		ScheduledAt:  job.ScheduledAt,
		StartedAt:    job.StartedAt,
		CompletedAt:  job.CompletedAt,
		WorkerID:     job.WorkerID,
		ErrorMessage: job.ErrorMessage,
		Cron:         job.CronExpression,
	}, nil
}

// ---------------------------------------------------------------------------
// DeleteJob
// ---------------------------------------------------------------------------

// DeleteJob removes a job from the system.
func (c *Core) DeleteJob(ctx context.Context, id uuid.UUID) error {
	// Get the job first to check status
	job, err := c.store.GetJob(ctx, id)
	if err != nil {
		return err
	}

	// Cannot delete running jobs
	if job.Status == models.JobStatusRunning {
		return fmt.Errorf("cannot delete a running job")
	}

	return c.store.DeleteJob(ctx, id)
}

// ---------------------------------------------------------------------------
// RetryJob
// ---------------------------------------------------------------------------

// RetryJob manually retries a failed or DLQ job.
func (c *Core) RetryJob(ctx context.Context, id uuid.UUID) error {
	job, err := c.store.GetJob(ctx, id)
	if err != nil {
		return err
	}

	if !job.Status.IsTerminal() {
		return fmt.Errorf("job status %s does not allow retry", job.Status)
	}

	// Reset job for retry
	job.RetryCount = 0
	job.Status = models.JobStatusPending
	job.ErrorMessage = ""
	job.WorkerID = ""
	job.ScheduledAt = time.Now().UTC()
	job.StartedAt = nil
	job.CompletedAt = nil

	// Update in store
	if err := c.store.UpdateJobStatus(ctx, job.ID, models.JobStatusPending); err != nil {
		return fmt.Errorf("update job status: %w", err)
	}

	// Re-enqueue
	if err := c.queue.Enqueue(ctx, job); err != nil {
		return fmt.Errorf("enqueue job: %w", err)
	}

	c.metrics.RecordJobRetried(job.Type)
	c.logger.Info("job manually retried", zap.String("job_id", job.ID.String()))

	return nil
}

// ---------------------------------------------------------------------------
// Delayed Job Loop
// ---------------------------------------------------------------------------

// delayedJobLoop periodically moves delayed jobs to ready queues.
func (c *Core) delayedJobLoop(ctx context.Context) {
	defer c.cronWg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.cronStop:
			return
		case <-ticker.C:
			c.processDelayedJobs(ctx)
		}
	}
}

// processDelayedJobs moves jobs from the delayed queue to ready queues.
func (c *Core) processDelayedJobs(ctx context.Context) {
	start := time.Now()

	// Try to acquire lock for delayed job processing
	lockKey := "delayed-job-processor"
	token, err := c.locker.Acquire(ctx, lockKey, 5*time.Second)
	if err != nil {
		return // Another scheduler is processing
	}
	defer c.locker.Release(ctx, lockKey, token)

	// Move delayed jobs
	moved, err := c.queue.MoveDelayed(ctx, 1000)
	if err != nil {
		c.logger.Error("failed to move delayed jobs", zap.Error(err))
		return
	}

	if moved > 0 {
		c.metrics.RecordDelayedJobsMoved(moved)
		c.metrics.RecordSchedulerLoop(time.Since(start))
		c.logger.Debug("moved delayed jobs", zap.Int("count", moved))
	}
}

// ---------------------------------------------------------------------------
// Reclaim Loop
// ---------------------------------------------------------------------------

// reclaimLoop periodically reclaims orphaned in-flight jobs.
func (c *Core) reclaimLoop(ctx context.Context) {
	defer c.cronWg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.cronStop:
			return
		case <-ticker.C:
			c.reclaimOrphanedJobs(ctx)
		}
	}
}

// reclaimOrphanedJobs returns jobs from crashed workers to the queue.
func (c *Core) reclaimOrphanedJobs(ctx context.Context) {
	reclaimed, err := c.queue.ReclaimOrphaned(ctx, 2*time.Minute)
	if err != nil {
		c.logger.Error("failed to reclaim orphaned jobs", zap.Error(err))
		return
	}

	if reclaimed > 0 {
		c.logger.Info("reclaimed orphaned jobs", zap.Int("count", reclaimed))
	}
}

// ---------------------------------------------------------------------------
// Cron Loop
// ---------------------------------------------------------------------------

// cronLoop manages recurring cron jobs.
func (c *Core) cronLoop(ctx context.Context) {
	defer c.cronWg.Done()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.cronStop:
			return
		case <-ticker.C:
			c.processCronJobs(ctx)
		}
	}
}

// processCronJobs finds and schedules due cron jobs.
func (c *Core) processCronJobs(ctx context.Context) {
	// Get active cron jobs from database
	status := models.JobStatusPending
	// For cron jobs, we look for jobs with cron_expression set
	// In a full implementation, we'd have a separate cron_jobs table
	// Here we check if any cron jobs need their next run scheduled

	c.cronMu.RLock()
	cronJobList := make([]*models.Job, 0, len(c.cronJobs))
	for _, job := range c.cronJobs {
		cronJobList = append(cronJobList, job)
	}
	c.cronMu.RUnlock()

	now := time.Now().UTC()
	for _, job := range cronJobList {
		if job.CronNextRun != nil && job.CronNextRun.Before(now) {
			c.scheduleCronInstance(ctx, job)
		}
	}
}

// scheduleCronInstance creates a job instance from a cron definition.
func (c *Core) scheduleCronInstance(ctx context.Context, cronJob *models.Job) {
	// Create a new job instance (not a cron definition)
	instance, err := models.NewJob(
		cronJob.Type,
		cronJob.Payload,
		models.WithPriority(cronJob.Priority),
		models.WithMaxRetries(cronJob.MaxRetries),
		models.WithTimeout(cronJob.Timeout),
	)
	if err != nil {
		c.logger.Error("failed to create cron instance",
			zap.String("cron_job_id", cronJob.ID.String()),
			zap.Error(err),
		)
		return
	}

	// Persist and enqueue
	if err := c.store.CreateJob(ctx, instance); err != nil {
		c.logger.Error("failed to persist cron instance",
			zap.String("job_id", instance.ID.String()),
			zap.Error(err),
		)
		return
	}

	if err := c.queue.Enqueue(ctx, instance); err != nil {
		c.logger.Error("failed to enqueue cron instance",
			zap.String("job_id", instance.ID.String()),
			zap.Error(err),
		)
		return
	}

	// Update next run time
	next, err := ParseCron(cronJob.CronExpression, time.Now().UTC())
	if err != nil {
		c.logger.Error("failed to calculate next cron run",
			zap.String("cron", cronJob.CronExpression),
			zap.Error(err),
		)
		return
	}

	cronJob.CronNextRun = &next

	c.metrics.RecordJobSubmitted(instance.Type, instance.Priority)
	c.logger.Info("cron job instance scheduled",
		zap.String("cron_job_id", cronJob.ID.String()),
		zap.String("instance_id", instance.ID.String()),
		zap.Time("next_run", next),
	)
}

// RegisterCronJob registers a cron job for periodic scheduling.
func (c *Core) RegisterCronJob(ctx context.Context, req *models.SubmitJobRequest) (*models.Job, error) {
	if req.Cron == "" {
		return nil, fmt.Errorf("cron expression is required")
	}

	// Parse and validate cron
	next, err := ParseCron(req.Cron, time.Now().UTC())
	if err != nil {
		return nil, fmt.Errorf("invalid cron expression: %w", err)
	}

	job, err := models.NewJob(
		req.Type,
		req.Payload,
		models.WithPriority(req.Priority),
		models.WithMaxRetries(req.MaxRetries),
		models.WithTimeout(req.Timeout),
		models.WithCron(req.Cron),
	)
	if err != nil {
		return nil, fmt.Errorf("create cron job: %w", err)
	}

	job.CronNextRun = &next
	job.Status = models.JobStatusPending // Cron definitions are also stored as jobs

	// Persist
	if err := c.store.CreateJob(ctx, job); err != nil {
		return nil, fmt.Errorf("persist cron job: %w", err)
	}

	// Register in memory
	c.cronMu.Lock()
	c.cronJobs[job.ID.String()] = job
	c.cronMu.Unlock()

	c.metrics.SetCronJobsActive(float64(len(c.cronJobs)))
	c.logger.Info("cron job registered",
		zap.String("job_id", job.ID.String()),
		zap.String("cron", req.Cron),
		zap.Time("next_run", next),
	)

	return job, nil
}

// ---------------------------------------------------------------------------
// ListJobs
// ---------------------------------------------------------------------------

// ListJobs returns jobs matching the given filter.
func (c *Core) ListJobs(ctx context.Context, filter store.JobFilter) ([]*models.Job, int64, error) {
	return c.store.ListJobs(ctx, filter)
}

// ---------------------------------------------------------------------------
// GetQueueStats
// ---------------------------------------------------------------------------

// GetQueueStats returns aggregate queue statistics.
func (c *Core) GetQueueStats(ctx context.Context) (*store.QueueStats, error) {
	return c.store.GetQueueStats(ctx)
}

// ---------------------------------------------------------------------------
// GetDLQ
// ---------------------------------------------------------------------------

// GetDLQ returns jobs from the dead-letter queue.
func (c *Core) GetDLQ(ctx context.Context, limit, offset int) ([]*models.Job, error) {
	return c.queue.GetDLQ(ctx, limit, offset)
}