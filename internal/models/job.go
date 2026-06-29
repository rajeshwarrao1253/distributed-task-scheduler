// Package models defines the core data structures for the distributed task scheduler.
// These models represent jobs, their lifecycle states, and related metadata used
// across all scheduler components.
package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// =============================================================================
// Errors
// =============================================================================

var (
	// ErrJobNotFound is returned when a job does not exist in the store.
	ErrJobNotFound = errors.New("job not found")

	// ErrJobAlreadyCompleted is returned when attempting to modify a completed job.
	ErrJobAlreadyCompleted = errors.New("job already completed")

	// ErrInvalidJobType is returned when the job type is empty or unknown.
	ErrInvalidJobType = errors.New("invalid job type")

	// ErrInvalidPriority is returned when priority is outside valid range [0, 9].
	ErrInvalidPriority = errors.New("priority must be between 0 and 9")

	// ErrInvalidCronExpression is returned when a cron expression cannot be parsed.
	ErrInvalidCronExpression = errors.New("invalid cron expression")

	// ErrJobTimeout is returned when a job exceeds its execution timeout.
	ErrJobTimeout = errors.New("job execution timed out")

	// ErrMaxRetriesExceeded is returned when a job exceeds its maximum retry count.
	ErrMaxRetriesExceeded = errors.New("maximum retry attempts exceeded")
)

// =============================================================================
// JobStatus
// =============================================================================

// JobStatus represents the current state of a job in its lifecycle.
type JobStatus string

const (
	// JobStatusPending indicates the job is queued and waiting for execution.
	JobStatusPending JobStatus = "pending"

	// JobStatusRunning indicates the job is currently being processed by a worker.
	JobStatusRunning JobStatus = "running"

	// JobStatusCompleted indicates the job executed successfully.
	JobStatusCompleted JobStatus = "completed"

	// JobStatusFailed indicates the job failed but may be retried.
	JobStatusFailed JobStatus = "failed"

	// JobStatusDeadLetter indicates the job failed and exceeded max retries.
	JobStatusDeadLetter JobStatus = "dead_letter"

	// JobStatusCancelled indicates the job was manually cancelled.
	JobStatusCancelled JobStatus = "cancelled"
)

// IsTerminal returns true if the status is a terminal state (no further transitions).
func (s JobStatus) IsTerminal() bool {
	switch s {
	case JobStatusCompleted, JobStatusDeadLetter, JobStatusCancelled:
		return true
	default:
		return false
	}
}

// CanTransitionTo returns true if transitioning from the current status to target is valid.
func (s JobStatus) CanTransitionTo(target JobStatus) bool {
	// Terminal states cannot transition
	if s.IsTerminal() {
		return false
	}

	switch s {
	case JobStatusPending:
		return target == JobStatusRunning || target == JobStatusCancelled
	case JobStatusRunning:
		return target == JobStatusCompleted || target == JobStatusFailed || target == JobStatusCancelled
	case JobStatusFailed:
		return target == JobStatusPending || target == JobStatusDeadLetter
	default:
		return false
	}
}

// =============================================================================
// Job
// =============================================================================

// Job represents a unit of work to be executed by the scheduler.
// It is the central data structure that flows through the entire system.
type Job struct {
	// ID is the unique identifier for this job.
	ID uuid.UUID `json:"id" db:"id"`

	// Type identifies the job handler that will process this job.
	// Examples: "send-email", "process-payment", "generate-report".
	Type string `json:"type" db:"job_type"`

	// Payload contains the job-specific data as JSON.
	// The handler interprets this payload based on the job type.
	Payload json.RawMessage `json:"payload" db:"payload"`

	// Priority determines dequeuing order. Lower values = higher priority (0-9).
	Priority int `json:"priority" db:"priority"`

	// Status is the current state of this job in its lifecycle.
	Status JobStatus `json:"status" db:"status"`

	// ScheduledAt is the earliest time this job can be executed.
	// Used for delayed job scheduling.
	ScheduledAt time.Time `json:"scheduled_at" db:"scheduled_at"`

	// RetryCount tracks how many times this job has been retried.
	RetryCount int `json:"retry_count" db:"retry_count"`

	// MaxRetries is the maximum number of retry attempts allowed.
	MaxRetries int `json:"max_retries" db:"max_retries"`

	// Timeout is the maximum execution time in milliseconds.
	Timeout int `json:"timeout" db:"timeout_ms"`

	// CronExpression, if set, defines the recurring schedule for this job.
	CronExpression string `json:"cron,omitempty" db:"cron_expression"`

	// CronNextRun is the next scheduled execution time for recurring jobs.
	CronNextRun *time.Time `json:"cron_next_run,omitempty" db:"cron_next_run"`

	// StartedAt is when the job began execution (nil if not started).
	StartedAt *time.Time `json:"started_at,omitempty" db:"started_at"`

	// CompletedAt is when the job finished execution (nil if not completed).
	CompletedAt *time.Time `json:"completed_at,omitempty" db:"completed_at"`

	// WorkerID identifies which worker is/was processing this job.
	WorkerID string `json:"worker_id,omitempty" db:"worker_id"`

	// ErrorMessage contains the last error if the job failed.
	ErrorMessage string `json:"error_message,omitempty" db:"error_message"`

	// CreatedAt is when the job was first submitted.
	CreatedAt time.Time `json:"created_at" db:"created_at"`

	// UpdatedAt is when the job was last modified.
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// NewJob creates a new Job with sensible defaults.
func NewJob(jobType string, payload json.RawMessage, opts ...JobOption) (*Job, error) {
	if jobType == "" {
		return nil, ErrInvalidJobType
	}

	job := &Job{
		ID:          uuid.New(),
		Type:        jobType,
		Payload:     payload,
		Priority:    5,
		Status:      JobStatusPending,
		ScheduledAt: time.Now().UTC(),
		MaxRetries:  3,
		Timeout:     30000,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}

	// Apply options
	for _, opt := range opts {
		opt(job)
	}

	// Validate
	if err := job.Validate(); err != nil {
		return nil, err
	}

	return job, nil
}

// Validate checks that the job has valid field values.
func (j *Job) Validate() error {
	if j.Type == "" {
		return ErrInvalidJobType
	}
	if j.Priority < 0 || j.Priority > 9 {
		return ErrInvalidPriority
	}
	if j.MaxRetries < 0 || j.MaxRetries > 20 {
		return fmt.Errorf("max_retries must be between 0 and 20")
	}
	if j.Timeout <= 0 || j.Timeout > 3600000 {
		return fmt.Errorf("timeout must be between 1 and 3600000 ms")
	}
	return nil
}

// IsRetryable returns true if the job can be retried.
func (j *Job) IsRetryable() bool {
	return j.RetryCount < j.MaxRetries && j.Status == JobStatusFailed
}

// NextRetryTime calculates the next retry time using exponential backoff.
// Formula: baseDelay * 2^retryCount + jitter
func (j *Job) NextRetryTime(baseDelay time.Duration) time.Time {
	backoff := baseDelay * (1 << j.RetryCount) // 2^retryCount
	// Add up to 1 second of jitter to prevent thundering herd
	jitter := time.Duration(j.ID.ID()%1000) * time.Millisecond
	return time.Now().UTC().Add(backoff + jitter)
}

// ExecutionTimeout returns the timeout as a time.Duration.
func (j *Job) ExecutionTimeout() time.Duration {
	return time.Duration(j.Timeout) * time.Millisecond
}

// =============================================================================
// JobOption
// =============================================================================

// JobOption is a functional option for configuring a new Job.
type JobOption func(*Job)

// WithPriority sets the job priority (0-9, lower = higher priority).
func WithPriority(p int) JobOption {
	return func(j *Job) {
		j.Priority = p
	}
}

// WithMaxRetries sets the maximum number of retry attempts.
func WithMaxRetries(n int) JobOption {
	return func(j *Job) {
		j.MaxRetries = n
	}
}

// WithTimeout sets the job execution timeout in milliseconds.
func WithTimeout(ms int) JobOption {
	return func(j *Job) {
		j.Timeout = ms
	}
}

// WithScheduledAt sets the scheduled execution time for delayed jobs.
func WithScheduledAt(t time.Time) JobOption {
	return func(j *Job) {
		j.ScheduledAt = t.UTC()
	}
}

// WithCron sets the cron expression for recurring jobs.
func WithCron(expr string) JobOption {
	return func(j *Job) {
		j.CronExpression = expr
	}
}

// =============================================================================
// JobHistory
// =============================================================================

// JobHistory records a status transition for audit purposes.
type JobHistory struct {
	ID           int64       `json:"id" db:"id"`
	JobID        uuid.UUID   `json:"job_id" db:"job_id"`
	OldStatus    *JobStatus  `json:"old_status,omitempty" db:"old_status"`
	NewStatus    JobStatus   `json:"new_status" db:"new_status"`
	WorkerID     string      `json:"worker_id,omitempty" db:"worker_id"`
	ErrorMessage string      `json:"error_message,omitempty" db:"error_message"`
	Metadata     json.RawMessage `json:"metadata" db:"metadata"`
	CreatedAt    time.Time   `json:"created_at" db:"created_at"`
}

// =============================================================================
// WorkerHeartbeat
// =============================================================================

// WorkerHeartbeat tracks the liveness of a worker process.
type WorkerHeartbeat struct {
	WorkerID       string    `json:"worker_id" db:"worker_id"`
	Hostname       string    `json:"hostname" db:"hostname"`
	Concurrency    int       `json:"concurrency" db:"concurrency"`
	ActiveJobs     int       `json:"active_jobs" db:"active_jobs"`
	LastHeartbeat  time.Time `json:"last_heartbeat" db:"last_heartbeat"`
	StartedAt      time.Time `json:"started_at" db:"started_at"`
}

// IsStale returns true if the heartbeat is older than the given threshold.
func (w *WorkerHeartbeat) IsStale(threshold time.Duration) bool {
	return time.Since(w.LastHeartbeat) > threshold
}

// =============================================================================
// SubmitJobRequest
// =============================================================================

// SubmitJobRequest is the API payload for submitting a new job.
type SubmitJobRequest struct {
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	Priority   int             `json:"priority,omitempty"`
	MaxRetries int             `json:"max_retries,omitempty"`
	Timeout    int             `json:"timeout,omitempty"`
	ScheduledAt *time.Time     `json:"scheduled_at,omitempty"`
	Cron       string          `json:"cron,omitempty"`
}

// Validate checks the submit request for required fields.
func (r *SubmitJobRequest) Validate() error {
	if r.Type == "" {
		return ErrInvalidJobType
	}
	if r.Priority < 0 || r.Priority > 9 {
		return ErrInvalidPriority
	}
	return nil
}

// =============================================================================
// JobResponse
// =============================================================================

// JobResponse is the API response for job operations.
type JobResponse struct {
	ID        uuid.UUID `json:"id"`
	Status    JobStatus `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// =============================================================================
// JobStatusResponse
// =============================================================================

// JobStatusResponse is the API response for job status queries.
type JobStatusResponse struct {
	ID           uuid.UUID  `json:"id"`
	Type         string     `json:"type"`
	Status       JobStatus  `json:"status"`
	Priority     int        `json:"priority"`
	RetryCount   int        `json:"retry_count"`
	MaxRetries   int        `json:"max_retries"`
	ScheduledAt  time.Time  `json:"scheduled_at"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	WorkerID     string     `json:"worker_id,omitempty"`
	ErrorMessage string     `json:"error_message,omitempty"`
	Cron         string     `json:"cron,omitempty"`
}