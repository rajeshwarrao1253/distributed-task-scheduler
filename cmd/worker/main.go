// cmd/worker is the main entry point for the worker process.
// It initializes and runs a worker pool that consumes jobs from the queue,
// executes them using registered handlers, and reports heartbeats.
//
// Usage:
//
//	go run cmd/worker/main.go
//
// Environment Variables:
//
//	REDIS_ADDR              - Redis server address (default: localhost:6379)
//	REDIS_PASSWORD          - Redis password (default: "")
//	REDIS_DB                - Redis database number (default: 0)
//	POSTGRES_DSN            - PostgreSQL connection string
//	WORKER_CONCURRENCY      - Number of parallel workers (default: 10)
//	WORKER_POLL_INTERVAL    - Queue poll interval (default: 1s)
//	WORKER_HEARTBEAT_INTERVAL - Heartbeat interval (default: 10s)
//	WORKER_ID               - Unique worker identifier (auto-generated if empty)
//	JOB_MAX_RETRIES         - Default max retries (default: 3)
//	JOB_TIMEOUT_MS          - Default job timeout in ms (default: 30000)
//	LOG_LEVEL               - Log level: debug, info, warn, error (default: info)
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/metrics"
	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/queue"
	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/store"
	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/worker"
)

var (
	version   = "dev"
	buildTime = "unknown"

	// CLI flags
	healthCheck = flag.Bool("health-check", false, "Run health check and exit")
)

func main() {
	flag.Parse()

	// Handle health check mode (for Docker HEALTHCHECK)
	if *healthCheck {
		if err := runHealthCheck(); err != nil {
			fmt.Fprintf(os.Stderr, "health check failed: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Initialize structured logging
	logger := initLogger(getEnv("LOG_LEVEL", "info"))
	defer logger.Sync()

	logger.Info("starting worker",
		zap.String("version", version),
		zap.String("build_time", buildTime),
		zap.Int("pid", os.Getpid()),
	)

	// Create root context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize components
	components, err := initializeComponents(ctx, logger)
	if err != nil {
		logger.Fatal("failed to initialize components", zap.Error(err))
	}

	// Register all job handlers
	handlers := registerHandlers(logger)
	logger.Info("registered handlers",
		zap.Strings("types", handlers.RegisteredTypes()),
	)

	// Create worker pool
	poolCfg := worker.Config{
		WorkerID:            getEnv("WORKER_ID", ""),
		Concurrency:         atoi(getEnv("WORKER_CONCURRENCY", "10")),
		PollInterval:        parseDuration(getEnv("WORKER_POLL_INTERVAL", "1s")),
		HeartbeatInterval:   parseDuration(getEnv("WORKER_HEARTBEAT_INTERVAL", "10s")),
		MaxJobExecutionTime: 5 * time.Minute,
		QueueEmptyBackoff:   100 * time.Millisecond,
	}

	pool := worker.NewPool(
		poolCfg,
		components.queue,
		components.pgStore,
		handlers,
		components.collector,
		logger,
	)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("received shutdown signal",
			zap.String("signal", sig.String()),
			zap.String("worker_id", poolCfg.WorkerID),
		)
		cancel()
		pool.Shutdown()
	}()

	// Start worker pool (blocks until shutdown)
	logger.Info("worker pool starting",
		zap.String("worker_id", poolCfg.WorkerID),
		zap.Int("concurrency", poolCfg.Concurrency),
	)
	if err := pool.Start(ctx); err != nil {
		logger.Fatal("worker pool exited", zap.Error(err))
	}

	logger.Info("worker stopped gracefully")
}

// =============================================================================
// Component Container
// =============================================================================

type components struct {
	queue     queue.Queue
	pgStore   store.Store
	collector *metrics.Collector
}

// initializeComponents creates and connects all worker components.
func initializeComponents(ctx context.Context, logger *zap.Logger) (*components, error) {
	// Initialize Redis queue
	queueCfg := queue.RedisConfig{
		Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
		Password: getEnv("REDIS_PASSWORD", ""),
		DB:       atoi(getEnv("REDIS_DB", "0")),
	}
	q, err := queue.NewRedisQueue(queueCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("redis queue init failed: %w", err)
	}
	logger.Info("connected to redis queue")

	// Initialize PostgreSQL store
	pgDSN := getEnv("POSTGRES_DSN", "postgres://scheduler:scheduler_pass@localhost:5432/taskscheduler?sslmode=disable")
	pgStore, err := store.NewPostgreSQLStore(ctx, pgDSN, logger)
	if err != nil {
		return nil, fmt.Errorf("postgresql connection failed: %w", err)
	}
	logger.Info("connected to postgresql")

	// Initialize metrics collector
	collector := metrics.NewCollector(logger)

	return &components{
		queue:     q,
		pgStore:   pgStore,
		collector: collector,
	}, nil
}

// =============================================================================
// Handler Registration
// =============================================================================

// registerHandlers creates and registers all available job handlers.
func registerHandlers(logger *zap.Logger) *worker.Registry {
	registry := worker.NewRegistry(logger)

	// Register built-in handlers
	registry.Register(worker.NewSendEmailHandler(logger))
	registry.Register(worker.NewProcessPaymentHandler(logger))
	registry.Register(worker.NewGenerateReportHandler(logger))
	registry.Register(worker.NewWebhookCallHandler(logger))
	registry.Register(worker.NewDataCleanupHandler(logger))

	return registry
}

// =============================================================================
// Helpers
// =============================================================================

// initLogger creates a production-ready zap logger.
func initLogger(level string) *zap.Logger {
	config := zap.NewProductionConfig()
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.DisableStacktrace = true

	switch level {
	case "debug":
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "warn":
		config.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		config.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	logger, err := config.Build()
	if err != nil {
		panic(fmt.Sprintf("failed to init logger: %v", err))
	}

	return logger
}

// getEnv returns environment variable value or default.
func getEnv(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

// atoi converts string to int, returns 0 on error.
func atoi(s string) int {
	var n int
	fmt.Sscanf(s, "%d", &n)
	return n
}

// parseDuration parses a duration string, returns 0 on error.
func parseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	return d
}

// runHealthCheck performs a simple health check.
func runHealthCheck() error {
	// Check if we can connect to Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
		Password: getEnv("REDIS_PASSWORD", ""),
		DB:       atoi(getEnv("REDIS_DB", "0")),
	})
	defer redisClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis health check failed: %w", err)
	}

	return nil
}