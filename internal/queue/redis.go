// Package queue provides Redis-backed queue operations for the distributed task scheduler.
// It supports priority queues, delayed jobs using sorted sets, dead-letter queues,
// and atomic push/pop operations to ensure job integrity.
//
// The queue design uses multiple Redis data structures:
//   - Lists: Ready queues per priority level (LPUSH/BRPOP)
//   - Sorted Sets: Delayed jobs scheduled for future execution
//   - Lists: Dead-letter queue for failed jobs
//   - Hashes: In-flight job tracking
//
// This design prioritizes throughput and simplicity while maintaining reliability.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/models"
)

// =============================================================================
// Constants
// =============================================================================

const (
	// Key prefixes for Redis namespaces
	keyPrefix       = "scheduler"
	readyQueueTpl   = "%s:queue:ready:p%d"    // ready queue per priority
	delayedQueueKey = "%s:queue:delayed"       // sorted set for delayed jobs
	dlqKey          = "%s:queue:dlq"           // dead letter queue
	inflightKeyTpl  = "%s:inflight:%s"         // in-flight job tracking per worker
	jobDataKeyTpl   = "%s:job:%s"              // job data hash

	// Script keys for atomic operations
	enqueueScript = `
		local jobKey = KEYS[1]
		local queueKey = KEYS[2]
		local delayedKey = KEYS[3]
		local jobData = ARGV[1]
		local priority = tonumber(ARGV[2])
		local scheduledAt = tonumber(ARGV[3])
		local now = tonumber(ARGV[4])

		-- Store job data
		redis.call('SET', jobKey, jobData)
		redis.call('EXPIRE', jobKey, 86400 * 7)  -- 7 day TTL

		-- Route to ready queue or delayed set
		if scheduledAt <= now then
			redis.call('LPUSH', queueKey, jobKey)
		else
			redis.call('ZADD', delayedKey, scheduledAt, jobKey)
		end
		return 1
	`

	dequeueScript = `
		local queueKeys = {}
		for i = 1, #KEYS - 1 do
			queueKeys[i] = KEYS[i]
		end
		local inflightKey = KEYS[#KEYS]
		local timeout = tonumber(ARGV[1])
		local workerID = ARGV[2]

		-- Try to get from ready queues (highest priority first)
		for i = 1, #queueKeys do
			local jobKey = redis.call('RPOP', queueKeys[i])
			if jobKey then
				-- Move to in-flight
				local expiry = redis.call('TIME')
				local expiryMs = (expiry[1] * 1000) + math.floor(expiry[2] / 1000) + timeout
				redis.call('ZADD', inflightKey, expiryMs, jobKey)
				-- Get job data
				local jobData = redis.call('GET', jobKey)
				return {jobKey, jobData}
			end
		end
		return nil
	`
)

// =============================================================================
// Errors
// =============================================================================

var (
	// ErrQueueEmpty is returned when no jobs are available.
	ErrQueueEmpty = errors.New("queue is empty")

	// ErrJobAlreadyExists is returned when attempting to enqueue a duplicate job.
	ErrJobAlreadyExists = errors.New("job already exists in queue")

	// ErrRedisUnavailable is returned when Redis connection fails.
	ErrRedisUnavailable = errors.New("redis unavailable")
)

// =============================================================================
// Queue Interface
// =============================================================================

// Queue defines the interface for job queue operations.
// Implementations may use Redis, in-memory, or other backends.
type Queue interface {
	// Enqueue adds a job to the queue. If scheduledAt is in the future,
	// the job is placed in the delayed queue instead.
	Enqueue(ctx context.Context, job *models.Job) error

	// Dequeue retrieves the highest-priority available job and marks it in-flight.
	// Blocks for up to timeout duration if the queue is empty.
	Dequeue(ctx context.Context, workerID string, timeout time.Duration) (*models.Job, error)

	// Acknowledge marks a job as completed and removes it from in-flight.
	Acknowledge(ctx context.Context, jobID uuid.UUID, workerID string) error

	// Requeue returns a failed job to the queue for retry.
	Requeue(ctx context.Context, job *models.Job) error

	// Nack (negative acknowledge) marks a job as failed and either requeues
	// for retry or moves to DLQ if max retries exceeded.
	Nack(ctx context.Context, job *models.Job, workerID string, errMsg string) error

	// MoveDelayed moves jobs from the delayed queue to ready queues
	// when their scheduled time has arrived.
	MoveDelayed(ctx context.Context, batchSize int) (int, error)

	// GetDLQ returns jobs from the dead-letter queue.
	GetDLQ(ctx context.Context, limit, offset int) ([]*models.Job, error)

	// QueueDepth returns the total number of pending jobs across all priorities.
	QueueDepth(ctx context.Context) (map[int]int64, error)

	// Close releases resources associated with the queue.
	Close() error
}

// =============================================================================
// RedisQueue
// =============================================================================

// RedisQueue implements the Queue interface using Redis.
type RedisQueue struct {
	client     *redis.Client
	keyPrefix  string
	logger     *zap.Logger

	// Pre-compiled Lua scripts for atomic operations
	enqueueSha string
	dequeueSha string
}

// RedisConfig holds configuration for Redis connection.
type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

// NewRedisQueue creates a new Redis-backed queue.
func NewRedisQueue(cfg RedisConfig, logger *zap.Logger) (*RedisQueue, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: 50,
		MinIdleConns: 10,
	})

	// Verify connectivity
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRedisUnavailable, err)
	}

	q := &RedisQueue{
		client:    client,
		keyPrefix: keyPrefix,
		logger:    logger.With(zap.String("component", "redis_queue")),
	}

	// Load Lua scripts into Redis
	if err := q.loadScripts(ctx); err != nil {
		return nil, fmt.Errorf("failed to load lua scripts: %w", err)
	}

	q.logger.Info("redis queue initialized", zap.String("addr", cfg.Addr))
	return q, nil
}

// loadScripts compiles and stores Lua scripts in Redis.
func (q *RedisQueue) loadScripts(ctx context.Context) error {
	sha, err := q.client.ScriptLoad(ctx, enqueueScript).Result()
	if err != nil {
		return fmt.Errorf("load enqueue script: %w", err)
	}
	q.enqueueSha = sha

	sha, err = q.client.ScriptLoad(ctx, dequeueScript).Result()
	if err != nil {
		return fmt.Errorf("load dequeue script: %w", err)
	}
	q.dequeueSha = sha

	return nil
}

// ---------------------------------------------------------------------------
// Enqueue
// ---------------------------------------------------------------------------

// Enqueue adds a job to the ready queue or delayed queue based on scheduled time.
func (q *RedisQueue) Enqueue(ctx context.Context, job *models.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	jobKey := fmt.Sprintf(jobDataKeyTpl, q.keyPrefix, job.ID.String())
	queueKey := fmt.Sprintf(readyQueueTpl, q.keyPrefix, job.Priority)
	delayedKey := fmt.Sprintf(delayedQueueKey, q.keyPrefix)

	scheduledAtMs := job.ScheduledAt.UnixMilli()
	nowMs := time.Now().UTC().UnixMilli()

	// Use Lua script for atomic operation
	args := []interface{}{
		string(data),       // ARGV[1]: job data
		job.Priority,       // ARGV[2]: priority
		scheduledAtMs,      // ARGV[3]: scheduled time
		nowMs,              // ARGV[4]: current time
	}

	keys := []string{jobKey, queueKey, delayedKey}

	err = q.client.EvalSha(ctx, q.enqueueSha, keys, args...).Err()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil
		}
		// Fallback: script may not be cached, retry with full script
		err = q.client.Eval(ctx, enqueueScript, keys, args...).Err()
		if err != nil && !errors.Is(err, redis.Nil) {
			return fmt.Errorf("enqueue job: %w", err)
		}
	}

	q.logger.Debug("job enqueued",
		zap.String("job_id", job.ID.String()),
		zap.String("type", job.Type),
		zap.Int("priority", job.Priority),
		zap.Time("scheduled_at", job.ScheduledAt),
	)
	return nil
}

// ---------------------------------------------------------------------------
// Dequeue
// ---------------------------------------------------------------------------

// Dequeue retrieves the highest-priority job and marks it in-flight.
func (q *RedisQueue) Dequeue(ctx context.Context, workerID string, timeout time.Duration) (*models.Job, error) {
	// Build priority queue keys (0 = highest priority)
	keys := make([]string, 11) // priorities 0-9 + inflight
	for p := 0; p <= 9; p++ {
		keys[p] = fmt.Sprintf(readyQueueTpl, q.keyPrefix, p)
	}
	keys[10] = fmt.Sprintf(inflightKeyTpl, q.keyPrefix, workerID)

	args := []interface{}{
		timeout.Milliseconds(),
		workerID,
	}

	result, err := q.client.EvalSha(ctx, q.dequeueSha, keys, args...).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrQueueEmpty
		}
		// Fallback to full script
		result, err = q.client.Eval(ctx, dequeueScript, keys, args...).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return nil, ErrQueueEmpty
			}
			return nil, fmt.Errorf("dequeue: %w", err)
		}
	}

	if result == nil {
		return nil, ErrQueueEmpty
	}

	slice, ok := result.([]interface{})
	if !ok || len(slice) < 2 {
		return nil, ErrQueueEmpty
	}

	jobKey, _ := slice[0].(string)
	jobData, _ := slice[1].(string)

	var job models.Job
	if err := json.Unmarshal([]byte(jobData), &job); err != nil {
		return nil, fmt.Errorf("unmarshal job data: %w", err)
	}

	q.logger.Debug("job dequeued",
		zap.String("job_id", job.ID.String()),
		zap.String("worker_id", workerID),
		zap.String("job_key", jobKey),
	)

	return &job, nil
}

// ---------------------------------------------------------------------------
// Acknowledge
// ---------------------------------------------------------------------------

// Acknowledge removes a job from in-flight and deletes its data.
func (q *RedisQueue) Acknowledge(ctx context.Context, jobID uuid.UUID, workerID string) error {
	jobKey := fmt.Sprintf(jobDataKeyTpl, q.keyPrefix, jobID.String())
	inflightKey := fmt.Sprintf(inflightKeyTpl, q.keyPrefix, workerID)

	pipe := q.client.Pipeline()
	pipe.Del(ctx, jobKey)
	pipe.ZRem(ctx, inflightKey, jobKey)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("acknowledge job: %w", err)
	}

	q.logger.Debug("job acknowledged",
		zap.String("job_id", jobID.String()),
		zap.String("worker_id", workerID),
	)
	return nil
}

// ---------------------------------------------------------------------------
// Requeue
// ---------------------------------------------------------------------------

// Requeue returns a failed job to the ready queue for retry.
func (q *RedisQueue) Requeue(ctx context.Context, job *models.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	jobKey := fmt.Sprintf(jobDataKeyTpl, q.keyPrefix, job.ID.String())
	queueKey := fmt.Sprintf(readyQueueTpl, q.keyPrefix, job.Priority)

	pipe := q.client.Pipeline()
	pipe.Set(ctx, jobKey, data, 7*24*time.Hour)
	pipe.LPush(ctx, queueKey, jobKey)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("requeue job: %w", err)
	}

	q.logger.Debug("job requeued",
		zap.String("job_id", job.ID.String()),
		zap.Int("retry_count", job.RetryCount),
	)
	return nil
}

// ---------------------------------------------------------------------------
// Nack (Negative Acknowledge)
// ---------------------------------------------------------------------------

// Nack handles a failed job: either requeue for retry or move to DLQ.
func (q *RedisQueue) Nack(ctx context.Context, job *models.Job, workerID string, errMsg string) error {
	jobKey := fmt.Sprintf(jobDataKeyTpl, q.keyPrefix, job.ID.String())
	inflightKey := fmt.Sprintf(inflightKeyTpl, q.keyPrefix, workerID)

	if job.IsRetryable() {
		// Requeue for retry
		job.RetryCount++
		job.Status = models.JobStatusPending
		job.ErrorMessage = errMsg
		job.ScheduledAt = job.NextRetryTime(5 * time.Second)

		return q.Requeue(ctx, job)
	}

	// Move to DLQ
	job.Status = models.JobStatusDeadLetter
	job.ErrorMessage = errMsg

	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	dlq := fmt.Sprintf(dlqKey, q.keyPrefix)

	pipe := q.client.Pipeline()
	pipe.Set(ctx, jobKey, data, 30*24*time.Hour) // 30 day TTL in DLQ
	pipe.LPush(ctx, dlq, jobKey)
	pipe.ZRem(ctx, inflightKey, jobKey)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("move to dlq: %w", err)
	}

	q.logger.Warn("job moved to dlq",
		zap.String("job_id", job.ID.String()),
		zap.Int("retry_count", job.RetryCount),
		zap.String("error", errMsg),
	)
	return nil
}

// ---------------------------------------------------------------------------
// MoveDelayed
// ---------------------------------------------------------------------------

// MoveDelayed moves jobs whose scheduled time has arrived from delayed queue
// to the appropriate ready queue.
func (q *RedisQueue) MoveDelayed(ctx context.Context, batchSize int) (int, error) {
	delayedKey := fmt.Sprintf(delayedQueueKey, q.keyPrefix)
	nowMs := float64(time.Now().UTC().UnixMilli())

	// Get jobs that are due
	jobKeys, err := q.client.ZRangeByScore(ctx, delayedKey, &redis.ZRangeBy{
		Min:   "0",
		Max:   strconv.FormatInt(time.Now().UTC().UnixMilli(), 10),
		Count: int64(batchSize),
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("get delayed jobs: %w", err)
	}

	if len(jobKeys) == 0 {
		return 0, nil
	}

	moved := 0
	pipe := q.client.Pipeline()

	for _, jobKey := range jobKeys {
		// Get job data to determine priority
		data, err := q.client.Get(ctx, jobKey).Result()
		if err != nil {
			q.logger.Warn("failed to get delayed job data",
				zap.String("job_key", jobKey),
				zap.Error(err),
			)
			pipe.ZRem(ctx, delayedKey, jobKey)
			continue
		}

		var job models.Job
		if err := json.Unmarshal([]byte(data), &job); err != nil {
			q.logger.Warn("failed to unmarshal delayed job",
				zap.String("job_key", jobKey),
				zap.Error(err),
			)
			pipe.ZRem(ctx, delayedKey, jobKey)
			continue
		}

		queueKey := fmt.Sprintf(readyQueueTpl, q.keyPrefix, job.Priority)
		pipe.LPush(ctx, queueKey, jobKey)
		pipe.ZRem(ctx, delayedKey, jobKey)
		moved++
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return moved, fmt.Errorf("move delayed jobs: %w", err)
	}

	if moved > 0 {
		q.logger.Debug("moved delayed jobs to ready queues",
			zap.Int("count", moved),
		)
	}

	return moved, nil
}

// ---------------------------------------------------------------------------
// GetDLQ
// ---------------------------------------------------------------------------

// GetDLQ returns jobs from the dead-letter queue.
func (q *RedisQueue) GetDLQ(ctx context.Context, limit, offset int) ([]*models.Job, error) {
	dlq := fmt.Sprintf(dlqKey, q.keyPrefix)

	// Get job keys from DLQ (newest first)
	jobKeys, err := q.client.LRange(ctx, dlq, int64(offset), int64(offset+limit-1)).Result()
	if err != nil {
		return nil, fmt.Errorf("get dlq keys: %w", err)
	}

	if len(jobKeys) == 0 {
		return []*models.Job{}, nil
	}

	// Fetch job data for each key
	pipe := q.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(jobKeys))
	for i, key := range jobKeys {
		cmds[i] = pipe.Get(ctx, key)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("get dlq job data: %w", err)
	}

	jobs := make([]*models.Job, 0, len(jobKeys))
	for i, cmd := range cmds {
		data, err := cmd.Result()
		if err != nil {
			q.logger.Warn("missing DLQ job data",
				zap.String("job_key", jobKeys[i]),
				zap.Error(err),
			)
			continue
		}

		var job models.Job
		if err := json.Unmarshal([]byte(data), &job); err != nil {
			q.logger.Warn("corrupt DLQ job data",
				zap.String("job_key", jobKeys[i]),
				zap.Error(err),
			)
			continue
		}

		jobs = append(jobs, &job)
	}

	return jobs, nil
}

// ---------------------------------------------------------------------------
// QueueDepth
// ---------------------------------------------------------------------------

// QueueDepth returns the number of pending jobs per priority level.
func (q *RedisQueue) QueueDepth(ctx context.Context) (map[int]int64, error) {
	depth := make(map[int]int64, 10)

	pipe := q.client.Pipeline()
	cmds := make([]*redis.IntCmd, 10)
	for p := 0; p <= 9; p++ {
		queueKey := fmt.Sprintf(readyQueueTpl, q.keyPrefix, p)
		cmds[p] = pipe.LLen(ctx, queueKey)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("get queue depths: %w", err)
	}

	for p := 0; p <= 9; p++ {
		depth[p] = cmds[p].Val()
	}

	return depth, nil
}

// ---------------------------------------------------------------------------
// ReclaimOrphaned
// ---------------------------------------------------------------------------

// ReclaimOrphaned returns in-flight jobs from stale workers back to ready queues.
// This prevents job loss when workers crash without acknowledging jobs.
func (q *RedisQueue) ReclaimOrphaned(ctx context.Context, maxAge time.Duration) (int, error) {
	// Find all in-flight keys
	pattern := fmt.Sprintf("%s:inflight:*", q.keyPrefix)
	iter := q.client.Scan(ctx, 0, pattern, 100).Iterator()

	reclaimed := 0
	for iter.Next(ctx) {
		inflightKey := iter.Val()
		nowMs := float64(time.Now().UTC().UnixMilli())
		cutoffMs := float64(time.Now().UTC().Add(-maxAge).UnixMilli())

		// Get expired entries from sorted set
		jobKeys, err := q.client.ZRangeByScore(ctx, inflightKey, &redis.ZRangeBy{
			Min: "0",
			Max: strconv.FormatInt(int64(cutoffMs), 10),
		}).Result()
		if err != nil {
			q.logger.Warn("failed to scan in-flight",
				zap.String("key", inflightKey),
				zap.Error(err),
			)
			continue
		}

		for _, jobKey := range jobKeys {
			// Get job data
			data, err := q.client.Get(ctx, jobKey).Result()
			if err != nil {
				q.logger.Warn("orphaned job data missing",
					zap.String("job_key", jobKey),
				)
				q.client.ZRem(ctx, inflightKey, jobKey)
				continue
			}

			var job models.Job
			if err := json.Unmarshal([]byte(data), &job); err != nil {
				q.logger.Warn("corrupt orphaned job data",
					zap.String("job_key", jobKey),
				)
				q.client.ZRem(ctx, inflightKey, jobKey)
				continue
			}

			// Requeue with incremented retry count
			job.RetryCount++
			job.Status = models.JobStatusPending
			job.WorkerID = ""
			job.ScheduledAt = job.NextRetryTime(5 * time.Second)

			if err := q.Requeue(ctx, &job); err != nil {
				q.logger.Error("failed to reclaim job",
					zap.String("job_key", jobKey),
					zap.Error(err),
				)
				continue
			}

			q.client.ZRem(ctx, inflightKey, jobKey)
			reclaimed++
		}

		// Clean up old in-flight keys
		count, _ := q.client.ZCard(ctx, inflightKey).Result()
		if count == 0 {
			q.client.Del(ctx, inflightKey)
		}
	}

	if err := iter.Err(); err != nil {
		return reclaimed, fmt.Errorf("scan in-flight keys: %w", err)
	}

	if reclaimed > 0 {
		q.logger.Info("reclaimed orphaned jobs",
			zap.Int("count", reclaimed),
			zap.Duration("max_age", maxAge),
		)
	}

	return reclaimed, nil
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

// Close closes the Redis connection.
func (q *RedisQueue) Close() error {
	return q.client.Close()
}