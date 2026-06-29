// Package lock provides distributed locking mechanisms using Redis.
// It implements the Redlock algorithm to prevent duplicate job execution
// across multiple scheduler or worker instances.
//
// The distributed lock is used for:
//   - Preventing concurrent execution of the same cron job
//   - Ensuring only one scheduler instance processes the delayed queue
//   - Protecting job status transitions from race conditions
package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// =============================================================================
// Errors
// =============================================================================

var (
	// ErrLockHeld is returned when the lock is already held by another process.
	ErrLockHeld = errors.New("lock is already held")

	// ErrLockNotHeld is returned when attempting to release a lock not owned.
	ErrLockNotHeld = errors.New("lock is not held by this process")

	// ErrLockTimeout is returned when the lock acquisition times out.
	ErrLockTimeout = errors.New("lock acquisition timed out")
)

// =============================================================================
// Locker Interface
// =============================================================================

// Locker defines the distributed locking interface.
type Locker interface {
	// Acquire attempts to acquire the lock with the given key.
	// Returns a token that must be provided to Release.
	Acquire(ctx context.Context, key string, ttl time.Duration) (token string, err error)

	// Release releases the lock. The token must match the one from Acquire.
	Release(ctx context.Context, key string, token string) error

	// Extend extends the lock TTL if still held.
	Extend(ctx context.Context, key string, token string, ttl time.Duration) error

	// IsHeld checks if the lock is currently held with the given token.
	IsHeld(ctx context.Context, key string, token string) (bool, error)
}

// =============================================================================
// RedisLocker
// =============================================================================

// RedisLocker implements distributed locking using Redis with the Redlock pattern.
type RedisLocker struct {
	client *redis.Client
	logger *zap.Logger
}

// NewRedisLocker creates a new Redis-based distributed locker.
func NewRedisLocker(client *redis.Client, logger *zap.Logger) *RedisLocker {
	return &RedisLocker{
		client: client,
		logger: logger.With(zap.String("component", "redis_lock")),
	}
}

// acquireScript is a Lua script that atomically acquires a lock only if
// the key does not exist or has expired. Returns 1 on success, 0 on failure.
const acquireScript = `
	if redis.call('EXISTS', KEYS[1]) == 0 then
		redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
		return 1
	end
	return 0
`

// releaseScript is a Lua script that releases a lock only if the token matches.
// This prevents releasing a lock acquired by another process.
const releaseScript = `
	if redis.call('GET', KEYS[1]) == ARGV[1] then
		return redis.call('DEL', KEYS[1])
	end
	return 0
`

// extendScript is a Lua script that extends the lock TTL if the token matches.
const extendScript = `
	if redis.call('GET', KEYS[1]) == ARGV[1] then
		return redis.call('PEXPIRE', KEYS[1], ARGV[2])
	end
	return 0
`

// ---------------------------------------------------------------------------
// Acquire
// ---------------------------------------------------------------------------

// Acquire attempts to acquire a distributed lock for the given key.
// Returns a unique token that must be used to release the lock.
func (l *RedisLocker) Acquire(ctx context.Context, key string, ttl time.Duration) (string, error) {
	// Generate a cryptographically secure random token
	token, err := generateToken()
	if err != nil {
		return "", fmt.Errorf("generate lock token: %w", err)
	}

	lockKey := fmt.Sprintf("scheduler:lock:%s", key)

	result, err := l.client.Eval(ctx, acquireScript, []string{lockKey}, token, ttl.Milliseconds()).Result()
	if err != nil {
		return "", fmt.Errorf("acquire lock: %w", err)
	}

	acquired, ok := result.(int64)
	if !ok || acquired != 1 {
		return "", ErrLockHeld
	}

	l.logger.Debug("lock acquired",
		zap.String("key", key),
		zap.Duration("ttl", ttl),
	)

	return token, nil
}

// ---------------------------------------------------------------------------
// Release
// ---------------------------------------------------------------------------

// Release releases the distributed lock. The token must match the one
// returned by Acquire to prevent releasing another process's lock.
func (l *RedisLocker) Release(ctx context.Context, key string, token string) error {
	lockKey := fmt.Sprintf("scheduler:lock:%s", key)

	result, err := l.client.Eval(ctx, releaseScript, []string{lockKey}, token).Result()
	if err != nil {
		return fmt.Errorf("release lock: %w", err)
	}

	released, ok := result.(int64)
	if !ok || released != 1 {
		return ErrLockNotHeld
	}

	l.logger.Debug("lock released", zap.String("key", key))
	return nil
}

// ---------------------------------------------------------------------------
// Extend
// ---------------------------------------------------------------------------

// Extend extends the TTL of an existing lock if the token matches.
func (l *RedisLocker) Extend(ctx context.Context, key string, token string, ttl time.Duration) error {
	lockKey := fmt.Sprintf("scheduler:lock:%s", key)

	result, err := l.client.Eval(ctx, extendScript, []string{lockKey}, token, ttl.Milliseconds()).Result()
	if err != nil {
		return fmt.Errorf("extend lock: %w", err)
	}

	extended, ok := result.(int64)
	if !ok || extended != 1 {
		return ErrLockNotHeld
	}

	l.logger.Debug("lock extended",
		zap.String("key", key),
		zap.Duration("ttl", ttl),
	)
	return nil
}

// ---------------------------------------------------------------------------
// IsHeld
// ---------------------------------------------------------------------------

// IsHeld checks if the lock is held with the given token.
func (l *RedisLocker) IsHeld(ctx context.Context, key string, token string) (bool, error) {
	lockKey := fmt.Sprintf("scheduler:lock:%s", key)

	val, err := l.client.Get(ctx, lockKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, fmt.Errorf("check lock: %w", err)
	}

	return val == token, nil
}

// ---------------------------------------------------------------------------
// TryAcquire
// ---------------------------------------------------------------------------

// TryAcquire attempts to acquire a lock with retries and a timeout.
// It will retry at the specified interval until the context is cancelled
// or the lock is acquired.
func (l *RedisLocker) TryAcquire(
	ctx context.Context,
	key string,
	ttl time.Duration,
	retryInterval time.Duration,
) (token string, err error) {
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	for {
		token, err = l.Acquire(ctx, key, ttl)
		if err == nil {
			return token, nil
		}
		if !errors.Is(err, ErrLockHeld) {
			return "", err
		}

		select {
		case <-ctx.Done():
			return "", fmt.Errorf("%w: %v", ErrLockTimeout, ctx.Err())
		case <-ticker.C:
			// Retry
		}
	}
}

// ---------------------------------------------------------------------------
// DistributedLock - High-level helper
// ---------------------------------------------------------------------------

// DistributedLock provides a convenient wrapper around Locker with
// automatic lock extension and cleanup.
type DistributedLock struct {
	locker Locker
	key    string
	token  string
	ttl    time.Duration
	logger *zap.Logger
	done   chan struct{}
}

// NewDistributedLock creates a managed lock that auto-extends.
func NewDistributedLock(locker Locker, key string, ttl time.Duration, logger *zap.Logger) *DistributedLock {
	return &DistributedLock{
		locker: locker,
		key:    key,
		ttl:    ttl,
		logger: logger,
		done:   make(chan struct{}),
	}
}

// Acquire acquires and starts the auto-extend background goroutine.
func (dl *DistributedLock) Acquire(ctx context.Context) error {
	token, err := dl.locker.Acquire(ctx, dl.key, dl.ttl)
	if err != nil {
		return err
	}

	dl.token = token

	// Start background goroutine to extend the lock
	go dl.autoExtend()

	return nil
}

// autoExtend periodically extends the lock TTL.
func (dl *DistributedLock) autoExtend() {
	ticker := time.NewTicker(dl.ttl / 3) // Extend at 1/3 of TTL
	defer ticker.Stop()

	for {
		select {
		case <-dl.done:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := dl.locker.Extend(ctx, dl.key, dl.token, dl.ttl)
			cancel()
			if err != nil {
				dl.logger.Warn("failed to extend lock",
					zap.String("key", dl.key),
					zap.Error(err),
				)
				return
			}
		}
	}
}

// Release releases the lock and stops the auto-extend goroutine.
func (dl *DistributedLock) Release(ctx context.Context) error {
	close(dl.done)
	return dl.locker.Release(ctx, dl.key, dl.token)
}

// =============================================================================
// Helpers
// =============================================================================

// generateToken creates a cryptographically secure random token.
func generateToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}