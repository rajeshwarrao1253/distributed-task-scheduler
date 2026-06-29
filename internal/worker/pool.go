// Package worker provides the worker pool implementation for the distributed task scheduler.
// It manages concurrent job execution with configurable parallelism, panic recovery,
// heartbeat signaling, and graceful shutdown.
//
// The worker pool pattern used here is the "single-queue, multiple-workers" model
// where workers compete to pull jobs from a shared queue, providing natural load
// balancing without a central coordinator.
package worker

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/metrics"
	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/models"
	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/queue"
	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/retry"
	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/store"
)

// =============================================================================
// Config
// =============================================================================

// Config holds worker pool configuration.
type Config struct {
	// WorkerID uniquely identifies this worker instance.
	WorkerID string

	// Concurrency is the number of parallel job executors.
	Concurrency int

	// PollInterval is how often to check for new jobs when idle.
	PollInterval time.Duration

	// HeartbeatInterval is how often to report liveness.
	HeartbeatInterval time.Duration

	// MaxJobExecutionTime caps job execution (overrides per-job timeout).
	MaxJobExecutionTime time.Duration

	// QueueEmptyBackoff increases poll interval when queue is empty.
	QueueEmptyBackoff time.Duration
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	hostname, _ := os.Hostname()
	return Config{
		WorkerID:            fmt.Sprintf("%s-%d", hostname, os.Getpid()),
		Concurrency:         10,
		PollInterval:        1 * time.Second,
		HeartbeatInterval:   10 * time.Second,
		MaxJobExecutionTime: 5 * time.Minute,
		QueueEmptyBackoff:   100 * time.Millisecond,
	}
}

// =============================================================================
// Pool
// =============================================================================

// Pool manages a group of workers that process jobs from a queue.
type Pool struct {
	config    Config
	queue     queue.Queue
	store     store.Store
	handlers  *Registry
	metrics   *metrics.Collector
	tracker   *metrics.WorkerActivityTracker
	retry     *retry.Engine
	logger    *zap.Logger

	// Synchronization
	wg        sync.WaitGroup
	stopChan  chan struct{}
	stopOnce  sync.Once
	running   int32 // accessed atomically via sync/atomic semantics using mutex
	mu        sync.RWMutex
}

// NewPool creates a new worker pool.
func NewPool(
	cfg Config,
	q queue.Queue,
	s store.Store,
	handlers *Registry,
	m *metrics.Collector,
	logger *zap.Logger,
) *Pool {
	return &Pool{
		config:    cfg,
		queue:     q,
		store:     s,
		handlers:  handlers,
		metrics:   m,
		tracker:   m.NewWorkerTracker(cfg.WorkerID),
		retry:     retry.NewEngine(logger),
		logger:    logger.With(zap.String("component", "worker_pool"), zap.String("worker_id", cfg.WorkerID)),
		stopChan:  make(chan struct{}),
	}
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

// Start launches the worker pool. It blocks until Shutdown is called.
func (p *Pool) Start(ctx context.Context) error {
	p.logger.Info("starting worker pool",
		zap.Int("concurrency", p.config.Concurrency),
		zap.String("worker_id", p.config.WorkerID),
	)

	// Report initial heartbeat
	if err := p.heartbeat(ctx); err != nil {
		p.logger.Warn("initial heartbeat failed", zap.Error(err))
	}

	// Start heartbeat goroutine
	heartbeatCtx, heartbeatCancel := context.WithCancel(context.Background())
	defer heartbeatCancel()
	go p.heartbeatLoop(heartbeatCtx)

	// Start worker goroutines
	p.wg.Add(p.config.Concurrency)
	for i := 0; i < p.config.Concurrency; i++ {
		go p.worker(i)
	}

	p.tracker.SetActive(true)

	// Wait for shutdown signal
	<-p.stopChan

	p.logger.Info("worker pool shutting down")
	p.tracker.SetActive(false)

	// Wait for all workers to finish
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logger.Info("all workers stopped gracefully")
	case <-time.After(30 * time.Second):
		p.logger.Warn("worker shutdown timed out")
	}

	return nil
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

// Shutdown initiates graceful shutdown of the worker pool.
func (p *Pool) Shutdown() {
	p.stopOnce.Do(func() {
		close(p.stopChan)
	})
}

// ---------------------------------------------------------------------------
// Worker Loop
// ---------------------------------------------------------------------------

// worker is the main loop for a single worker goroutine.
func (p *Pool) worker(id int) {
	defer p.wg.Done()

	logger := p.logger.With(zap.Int("goroutine_id", id))
	logger.Debug("worker started")

	pollInterval := p.config.PollInterval

	for {
		select {
		case <-p.stopChan:
			logger.Debug("worker stopping")
			return
		default:
		}

		// Try to dequeue a job
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		job, err := p.queue.Dequeue(ctx, p.config.WorkerID, 5*time.Second)
		cancel()

		if err != nil {
			if err == queue.ErrQueueEmpty {
				// Back off and retry
				select {
				case <-p.stopChan:
					return
				case <-time.After(pollInterval):
					pollInterval += p.config.QueueEmptyBackoff
					if pollInterval > 30*time.Second {
						pollInterval = 30 * time.Second
					}
					continue
				}
			}
			logger.Error("dequeue error", zap.Error(err))
			time.Sleep(p.config.PollInterval)
			continue
		}

		// Reset poll interval on successful dequeue
		pollInterval = p.config.PollInterval

		// Execute the job
		p.executeJob(job)
	}
}

// ---------------------------------------------------------------------------
// Job Execution
// ---------------------------------------------------------------------------

// executeJob runs a single job with full error handling and metrics.
func (p *Pool) executeJob(job *models.Job) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), job.ExecutionTimeout())
	defer cancel()

	// Update job status to running in database
	if err := p.store.UpdateJobStatus(
		context.Background(),
		job.ID,
		models.JobStatusRunning,
		store.WithWorkerID(p.config.WorkerID),
		store.WithStartedAt(time.Now().UTC()),
	); err != nil {
		p.logger.Error("failed to update job status to running",
			zap.String("job_id", job.ID.String()),
			zap.Error(err),
		)
		// Continue anyway - the job is already dequeued
	}

	// Find the appropriate handler
	handler, ok := p.handlers.Get(job.Type)
	if !ok {
		p.handleJobError(job, fmt.Errorf("no handler registered for job type: %s", job.Type))
		return
	}

	// Execute with panic recovery
	success := p.runWithRecovery(ctx, handler, job)

	// Update metrics
	elapsed := time.Since(start)
	p.metrics.RecordJobExecutionTime(job.Type, elapsed)

	if success {
		p.handleJobSuccess(job, start)
	} else {
		p.handleJobError(job, ctx.Err())
	}
}

// runWithRecovery executes a handler with panic recovery.
func (p *Pool) runWithRecovery(ctx context.Context, handler JobHandler, job *models.Job) bool {
	done := make(chan bool, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				p.logger.Error("job handler panicked",
					zap.String("job_id", job.ID.String()),
					zap.String("job_type", job.Type),
					zap.Any("panic", r),
				)
				done <- false
			}
		}()

		err := handler.Execute(ctx, job)
		done <- (err == nil)
	}()

	select {
	case result := <-done:
		return result
	case <-ctx.Done():
		p.logger.Warn("job timed out",
			zap.String("job_id", job.ID.String()),
			zap.String("job_type", job.Type),
			zap.Duration("timeout", job.ExecutionTimeout()),
		)
		return false
	}
}

// ---------------------------------------------------------------------------
// Success / Error Handling
// ---------------------------------------------------------------------------

// handleJobSuccess processes a successfully completed job.
func (p *Pool) handleJobSuccess(job *models.Job, startTime time.Time) {
	ctx := context.Background()

	// Acknowledge in queue
	if err := p.queue.Acknowledge(ctx, job.ID, p.config.WorkerID); err != nil {
		p.logger.Error("failed to acknowledge job",
			zap.String("job_id", job.ID.String()),
			zap.Error(err),
		)
	}

	// Update database
	if err := p.store.UpdateJobStatus(
		ctx,
		job.ID,
		models.JobStatusCompleted,
		store.WithCompletedAt(time.Now().UTC()),
	); err != nil {
		p.logger.Error("failed to update job status to completed",
			zap.String("job_id", job.ID.String()),
			zap.Error(err),
		)
	}

	// Record metrics
	p.tracker.RecordJobProcessed(job.Type, true)
	p.metrics.RecordJobLatency(job.Type, time.Since(startTime))

	p.logger.Debug("job completed successfully",
		zap.String("job_id", job.ID.String()),
		zap.String("type", job.Type),
	)
}

// handleJobError processes a failed job, applying retry logic or DLQ routing.
func (p *Pool) handleJobError(job *models.Job, execErr error) {
	ctx := context.Background()
	errMsg := ""
	if execErr != nil {
		errMsg = execErr.Error()
	}

	// Negative acknowledge - this will either requeue or DLQ
	if err := p.queue.Nack(ctx, job, p.config.WorkerID, errMsg); err != nil {
		p.logger.Error("failed to nack job",
			zap.String("job_id", job.ID.String()),
			zap.Error(err),
		)
	}

	// Update database status
	status := models.JobStatusFailed
	if !job.IsRetryable() {
		status = models.JobStatusDeadLetter
		p.metrics.RecordJobDLQ(job.Type)
	} else {
		p.metrics.RecordJobRetried(job.Type)
	}

	if err := p.store.UpdateJobStatus(
		ctx,
		job.ID,
		status,
		store.WithErrorMessage(errMsg),
	); err != nil {
		p.logger.Error("failed to update job status to failed",
			zap.String("job_id", job.ID.String()),
			zap.Error(err),
		)
	}

	p.tracker.RecordJobProcessed(job.Type, false)

	p.logger.Warn("job failed",
		zap.String("job_id", job.ID.String()),
		zap.String("type", job.Type),
		zap.Int("retry_count", job.RetryCount),
		zap.Int("max_retries", job.MaxRetries),
		zap.Bool("will_retry", job.IsRetryable()),
		zap.Error(execErr),
	)
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

// heartbeatLoop periodically reports worker liveness.
func (p *Pool) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(p.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.heartbeat(ctx); err != nil {
				p.logger.Warn("heartbeat failed", zap.Error(err))
			}
		}
	}
}

// heartbeat reports the current worker status.
func (p *Pool) heartbeat(ctx context.Context) error {
	hb := &models.WorkerHeartbeat{
		WorkerID:      p.config.WorkerID,
		Hostname:      p.config.WorkerID, // WorkerID includes hostname
		Concurrency:   p.config.Concurrency,
		ActiveJobs:    0, // Could be tracked if needed
		LastHeartbeat: time.Now().UTC(),
		StartedAt:     time.Now().UTC(), // TODO: track actual start time
	}

	return p.store.RecordHeartbeat(ctx, hb)
}