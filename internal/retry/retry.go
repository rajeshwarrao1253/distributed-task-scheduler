// Package retry implements the retry engine for the distributed task scheduler.
// It provides configurable retry policies with exponential backoff, jitter,
// and routing to the dead-letter queue when retries are exhausted.
//
// The retry formula is: delay = baseDelay * 2^attempt + random_jitter
// where random_jitter is a value between 0 and maxJitter.
package retry

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"go.uber.org/zap"

	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/models"
)

// =============================================================================
// Constants
// =============================================================================

const (
	// DefaultBaseDelay is the initial delay between retries.
	DefaultBaseDelay = 5 * time.Second

	// DefaultMaxDelay caps the maximum delay between retries.
	DefaultMaxDelay = 30 * time.Minute

	// DefaultMaxJitter adds randomness to prevent thundering herd.
	DefaultMaxJitter = 1 * time.Second

	// DefaultMaxRetries is the default maximum number of retry attempts.
	DefaultMaxRetries = 3
)

// =============================================================================
// Policy
// =============================================================================

// Policy defines the retry behavior for a job.
type Policy struct {
	// MaxRetries is the maximum number of retry attempts.
	MaxRetries int

	// BaseDelay is the initial delay between retries.
	BaseDelay time.Duration

	// MaxDelay caps the maximum delay between retries.
	MaxDelay time.Duration

	// MaxJitter adds randomness to each retry delay.
	MaxJitter time.Duration

	// Retryable determines if a specific error is retryable.
	// If nil, all errors are considered retryable.
	Retryable func(error) bool
}

// DefaultPolicy returns a policy with sensible defaults.
func DefaultPolicy() Policy {
	return Policy{
		MaxRetries: DefaultMaxRetries,
		BaseDelay:  DefaultBaseDelay,
		MaxDelay:   DefaultMaxDelay,
		MaxJitter:  DefaultMaxJitter,
		Retryable:  nil, // All errors are retryable by default
	}
}

// AggressivePolicy returns a policy with faster retries for transient errors.
func AggressivePolicy() Policy {
	return Policy{
		MaxRetries: 5,
		BaseDelay:  1 * time.Second,
		MaxDelay:   5 * time.Minute,
		MaxJitter:  500 * time.Millisecond,
		Retryable:  nil,
	}
}

// ConservativePolicy returns a policy with slower retries for external services.
func ConservativePolicy() Policy {
	return Policy{
		MaxRetries: 3,
		BaseDelay:  1 * time.Minute,
		MaxDelay:   1 * time.Hour,
		MaxJitter:  10 * time.Second,
		Retryable:  nil,
	}
}

// =============================================================================
// Engine
// =============================================================================

// Engine executes retry policies and makes routing decisions.
type Engine struct {
	logger *zap.Logger
}

// NewEngine creates a new retry engine.
func NewEngine(logger *zap.Logger) *Engine {
	return &Engine{
		logger: logger.With(zap.String("component", "retry_engine")),
	}
}

// ShouldRetry determines if a job should be retried based on the policy.
// Returns the delay to wait before the next retry, or an error if retries
// are exhausted or the error is not retryable.
func (e *Engine) ShouldRetry(job *models.Job, execErr error, policy Policy) (time.Duration, error) {
	// Check if error is retryable
	if policy.Retryable != nil && !policy.Retryable(execErr) {
		return 0, fmt.Errorf("non-retryable error: %w", execErr)
	}

	// Check if max retries exceeded
	if job.RetryCount >= policy.MaxRetries {
		return 0, fmt.Errorf("%w: retry_count=%d, max_retries=%d",
			models.ErrMaxRetriesExceeded, job.RetryCount, policy.MaxRetries)
	}

	// Calculate backoff delay
	delay := calculateBackoff(job.RetryCount, policy)

	e.logger.Debug("job retry scheduled",
		zap.String("job_id", job.ID.String()),
		zap.Int("retry_count", job.RetryCount),
		zap.Int("max_retries", policy.MaxRetries),
		zap.Duration("delay", delay),
		zap.Error(execErr),
	)

	return delay, nil
}

// calculateBackoff computes the exponential backoff with jitter.
// Formula: min(base * 2^attempt, maxDelay) + random(0, maxJitter)
func calculateBackoff(attempt int, policy Policy) time.Duration {
	// Calculate exponential component: base * 2^attempt
	backoff := policy.BaseDelay * (1 << attempt)

	// Cap at max delay
	if backoff > policy.MaxDelay {
		backoff = policy.MaxDelay
	}

	// Add jitter to prevent thundering herd
	if policy.MaxJitter > 0 {
		jitter := time.Duration(rand.Int63n(int64(policy.MaxJitter)))
		backoff += jitter
	}

	return backoff
}

// =============================================================================
// PolicyForJob
// =============================================================================

// PolicyForJob selects the appropriate retry policy based on job type.
// This allows different job types to have different retry behaviors.
type PolicyForJob func(jobType string) Policy

// DefaultPolicyForJob returns a default policy selector.
func DefaultPolicyForJob() PolicyForJob {
	policies := map[string]Policy{
		"send-email":      DefaultPolicy(),
		"process-payment": AggressivePolicy(),
		"generate-report": ConservativePolicy(),
		"webhook-call":    AggressivePolicy(),
	}

	return func(jobType string) Policy {
		if p, ok := policies[jobType]; ok {
			return p
		}
		return DefaultPolicy()
	}
}

// =============================================================================
// Retry Executor
// =============================================================================

// Executor wraps a function with retry logic.
type Executor struct {
	policy Policy
	logger *zap.Logger
}

// NewExecutor creates a new retry executor.
func NewExecutor(policy Policy, logger *zap.Logger) *Executor {
	return &Executor{
		policy: policy,
		logger: logger.With(zap.String("component", "retry_executor")),
	}
}

// Execute runs the given function with retries.
// It blocks until the function succeeds or retries are exhausted.
func (e *Executor) Execute(ctx context.Context, fn func() error) error {
	var lastErr error

	for attempt := 0; attempt <= e.policy.MaxRetries; attempt++ {
		if err := fn(); err != nil {
			lastErr = err

			// Check if this error is retryable
			if e.policy.Retryable != nil && !e.policy.Retryable(err) {
				return fmt.Errorf("non-retryable error on attempt %d: %w", attempt, err)
			}

			// Don't retry after the last attempt
			if attempt >= e.policy.MaxRetries {
				break
			}

			// Calculate backoff
			delay := calculateBackoff(attempt, e.policy)

			e.logger.Warn("attempt failed, retrying",
				zap.Int("attempt", attempt+1),
				zap.Int("max_retries", e.policy.MaxRetries),
				zap.Duration("delay", delay),
				zap.Error(err),
			)

			// Wait with context cancellation support
			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during retry: %w", ctx.Err())
			case <-time.After(delay):
				// Continue to next attempt
			}
		} else {
			return nil
		}
	}

	return fmt.Errorf("all %d retries exhausted: %w", e.policy.MaxRetries, lastErr)
}

// ExecuteWithResult runs the given function with retries and returns its result.
func ExecuteWithResult[T any](ctx context.Context, policy Policy, fn func() (T, error), logger *zap.Logger) (T, error) {
	var zero T
	var lastErr error

	for attempt := 0; attempt <= policy.MaxRetries; attempt++ {
		result, err := fn()
		if err != nil {
			lastErr = err

			if policy.Retryable != nil && !policy.Retryable(err) {
				return zero, fmt.Errorf("non-retryable error on attempt %d: %w", attempt, err)
			}

			if attempt >= policy.MaxRetries {
				break
			}

			delay := calculateBackoff(attempt, policy)
			logger.Warn("attempt failed, retrying",
				zap.Int("attempt", attempt+1),
				zap.Int("max_retries", policy.MaxRetries),
				zap.Duration("delay", delay),
				zap.Error(err),
			)

			select {
			case <-ctx.Done():
				return zero, fmt.Errorf("context cancelled during retry: %w", ctx.Err())
			case <-time.After(delay):
			}
		} else {
			return result, nil
		}
	}

	return zero, fmt.Errorf("all %d retries exhausted: %w", policy.MaxRetries, lastErr)
}

// =============================================================================
// Circuit Breaker (Optional Enhancement)
// =============================================================================

// CircuitState represents the state of a circuit breaker.
type CircuitState int

const (
	CircuitClosed CircuitState = iota   // Normal operation
	CircuitOpen                         // Failing, reject requests
	CircuitHalfOpen                     // Testing if service recovered
)

// CircuitBreaker prevents repeated retries when a service is down.
type CircuitBreaker struct {
	failures    int
	threshold   int
	timeout     time.Duration
	lastFailure time.Time
	state       CircuitState
	mu          chan struct{} // Lightweight mutex
	logger      *zap.Logger
}

// NewCircuitBreaker creates a new circuit breaker.
func NewCircuitBreaker(threshold int, timeout time.Duration, logger *zap.Logger) *CircuitBreaker {
	return &CircuitBreaker{
		threshold: threshold,
		timeout:   timeout,
		state:     CircuitClosed,
		mu:        make(chan struct{}, 1),
		logger:    logger.With(zap.String("component", "circuit_breaker")),
	}
}

// Allow returns true if the request should be allowed through.
func (cb *CircuitBreaker) Allow() bool {
	select {
	case cb.mu <- struct{}{}:
		defer func() { <-cb.mu }()

		switch cb.state {
		case CircuitClosed:
			return true
		case CircuitOpen:
			if time.Since(cb.lastFailure) > cb.timeout {
				cb.state = CircuitHalfOpen
				cb.logger.Info("circuit breaker entering half-open state")
				return true
			}
			return false
		case CircuitHalfOpen:
			return true
		}
		return true
	default:
		return false
	}
}

// RecordSuccess marks a request as successful.
func (cb *CircuitBreaker) RecordSuccess() {
	select {
	case cb.mu <- struct{}{}:
		defer func() { <-cb.mu }()

		cb.failures = 0
		if cb.state == CircuitHalfOpen {
			cb.state = CircuitClosed
			cb.logger.Info("circuit breaker closed")
		}
	default:
	}
}

// RecordFailure marks a request as failed.
func (cb *CircuitBreaker) RecordFailure() {
	select {
	case cb.mu <- struct{}{}:
		defer func() { <-cb.mu }()

		cb.failures++
		cb.lastFailure = time.Now()

		if cb.failures >= cb.threshold {
			if cb.state != CircuitOpen {
				cb.state = CircuitOpen
				cb.logger.Warn("circuit breaker opened",
					zap.Int("failures", cb.failures),
					zap.Duration("timeout", cb.timeout),
				)
			}
		}
	default:
	}
}

// State returns the current circuit breaker state.
func (cb *CircuitBreaker) State() CircuitState {
	select {
	case cb.mu <- struct{}{}:
		defer func() { <-cb.mu }()
		return cb.state
	default:
		return CircuitOpen // Default to open if can't acquire lock
	}
}