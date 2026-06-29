// cmd/scheduler is the main entry point for the scheduler process.
// It initializes and runs the HTTP API server, cron engine, job queue management,
// and scheduling logic.
//
// Usage:
//
//	go run cmd/scheduler/main.go
//
// Environment Variables:
//
//	REDIS_ADDR              - Redis server address (default: localhost:6379)
//	REDIS_PASSWORD          - Redis password (default: "")
//	REDIS_DB                - Redis database number (default: 0)
//	POSTGRES_DSN            - PostgreSQL connection string
//	SCHEDULER_HTTP_ADDR     - HTTP server bind address (default: :8080)
//	METRICS_ADDR            - Prometheus metrics address (default: :9090)
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

	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/lock"
	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/metrics"
	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/queue"
	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/scheduler"
	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/store"
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

	logger.Info("starting scheduler",
		zap.String("version", version),
		zap.String("build_time", buildTime),
	)

	// Create root context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize components
	components, err := initializeComponents(ctx, logger)
	if err != nil {
		logger.Fatal("failed to initialize components", zap.Error(err))
	}

	// Start scheduler core in background
	go func() {
		if err := components.core.Start(ctx); err != nil {
			logger.Error("scheduler core exited", zap.Error(err))
		}
	}()

	// Start metrics HTTP server in background
	metricsAddr := getEnv("METRICS_ADDR", ":9090")
	if metricsAddr != "" {
		go func() {
			logger.Info("starting metrics server", zap.String("addr", metricsAddr))
			mux := http.NewServeMux()
			mux.Handle("/metrics", components.collector.HTTPHandler())
			mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"status":"healthy"}`))
			})
			if err := http.ListenAndServe(metricsAddr, mux); err != nil {
				logger.Error("metrics server exited", zap.Error(err))
			}
		}()
	}

	// Start main API server
	apiAddr := getEnv("SCHEDULER_HTTP_ADDR", ":8080")
	server := scheduler.NewServer(components.core, components.collector, apiAddr, logger)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("received shutdown signal", zap.String("signal", sig.String()))
		cancel()
		components.core.Shutdown()
	}()

	logger.Info("scheduler ready", zap.String("api_addr", apiAddr))
	if err := server.Start(); err != nil {
		logger.Fatal("server exited", zap.Error(err))
	}
}

// =============================================================================
// Component Container
// =============================================================================

type components struct {
	queue     queue.Queue
	pgStore   store.Store
	locker    lock.Locker
	core      *scheduler.Core
	collector *metrics.Collector
}

// initializeComponents creates and connects all scheduler components.
func initializeComponents(ctx context.Context, logger *zap.Logger) (*components, error) {
	// Initialize Redis client (shared between queue and lock)
	redisClient := redis.NewClient(&redis.Options{
		Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
		Password: getEnv("REDIS_PASSWORD", ""),
		DB:       atoi(getEnv("REDIS_DB", "0")),
		PoolSize: 50,
	})

	// Test Redis connectivity
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}
	logger.Info("connected to redis")

	// Initialize PostgreSQL store
	pgDSN := getEnv("POSTGRES_DSN", "postgres://scheduler:scheduler_pass@localhost:5432/taskscheduler?sslmode=disable")
	pgStore, err := store.NewPostgreSQLStore(ctx, pgDSN, logger)
	if err != nil {
		return nil, fmt.Errorf("postgresql connection failed: %w", err)
	}
	logger.Info("connected to postgresql")

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

	// Initialize distributed locker
	locker := lock.NewRedisLocker(redisClient, logger)

	// Initialize metrics collector
	collector := metrics.NewCollector(logger)

	// Initialize scheduler core
	core := scheduler.NewCore(q, pgStore, locker, collector, logger)

	return &components{
		queue:     q,
		pgStore:   pgStore,
		locker:    locker,
		core:      core,
		collector: collector,
	}, nil
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

// runHealthCheck performs a simple health check.
func runHealthCheck() error {
	addr := getEnv("SCHEDULER_HTTP_ADDR", ":8080")
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://localhost%s/health", addr))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health endpoint returned %d", resp.StatusCode)
	}
	return nil
}