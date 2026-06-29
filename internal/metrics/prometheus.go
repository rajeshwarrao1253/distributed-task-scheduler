// Package metrics provides Prometheus instrumentation for the distributed task scheduler.
// It tracks job lifecycle events, worker activity, queue depth, and latency distributions
// for operational visibility and alerting.
package metrics

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// =============================================================================
// Metrics Namespace and Labels
// =============================================================================

const (
	namespace = "scheduler"

	// Label names
	LabelStatus   = "status"
	LabelJobType  = "job_type"
	LabelPriority = "priority"
	LabelWorkerID = "worker_id"
)

// =============================================================================
// Collector
// =============================================================================

// Collector holds all Prometheus metrics for the scheduler.
// Use NewCollector to create a properly initialized instance.
type Collector struct {
	registry *prometheus.Registry

	// Job lifecycle counters
	JobsSubmitted *prometheus.CounterVec
	JobsExecuted  *prometheus.CounterVec
	JobsFailed    *prometheus.CounterVec
	JobsRetried   *prometheus.CounterVec
	JobsDLQ       *prometheus.CounterVec
	JobsCancelled *prometheus.CounterVec

	// Timing histograms
	JobLatency       *prometheus.HistogramVec
	JobExecutionTime *prometheus.HistogramVec
	EnqueueLatency   prometheus.Histogram
	DequeueLatency   prometheus.Histogram

	// Gauges
	WorkersActive  *prometheus.GaugeVec
	QueueDepth     *prometheus.GaugeVec
	JobsInflight   prometheus.Gauge
	CronJobsActive prometheus.Gauge

	// Scheduler operations
	SchedulerLoopDuration prometheus.Histogram
	DelayedJobsMoved      prometheus.Counter

	// Retry metrics
	RetryAttempts    *prometheus.CounterVec
	CircuitBreakerState *prometheus.GaugeVec

	logger *zap.Logger
}

// NewCollector creates and registers all metrics.
func NewCollector(logger *zap.Logger) *Collector {
	registry := prometheus.NewRegistry()

	c := &Collector{
		registry: registry,
		logger:   logger.With(zap.String("component", "metrics")),

		JobsSubmitted: promauto.With(registry).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "jobs_submitted_total",
				Help:      "Total number of jobs submitted to the scheduler",
			},
			[]string{LabelJobType, LabelPriority},
		),

		JobsExecuted: promauto.With(registry).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "jobs_executed_total",
				Help:      "Total number of jobs executed by workers",
			},
			[]string{LabelJobType, LabelStatus},
		),

		JobsFailed: promauto.With(registry).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "jobs_failed_total",
				Help:      "Total number of jobs that failed execution",
			},
			[]string{LabelJobType, LabelWorkerID},
		),

		JobsRetried: promauto.With(registry).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "jobs_retried_total",
				Help:      "Total number of job retry attempts",
			},
			[]string{LabelJobType},
		),

		JobsDLQ: promauto.With(registry).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "jobs_dlq_total",
				Help:      "Total number of jobs sent to dead-letter queue",
			},
			[]string{LabelJobType},
		),

		JobsCancelled: promauto.With(registry).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "jobs_cancelled_total",
				Help:      "Total number of jobs cancelled",
			},
			[]string{LabelJobType},
		),

		JobLatency: promauto.With(registry).NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "job_latency_seconds",
				Help:      "End-to-end job latency from submission to completion",
				Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms to ~17min
			},
			[]string{LabelJobType},
		),

		JobExecutionTime: promauto.With(registry).NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "job_execution_duration_seconds",
				Help:      "Time spent executing a job",
				Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20),
			},
			[]string{LabelJobType},
		),

		EnqueueLatency: promauto.With(registry).NewHistogram(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "enqueue_latency_seconds",
				Help:      "Time taken to enqueue a job",
				Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15),
			},
		),

		DequeueLatency: promauto.With(registry).NewHistogram(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "dequeue_latency_seconds",
				Help:      "Time taken to dequeue a job",
				Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15),
			},
		),

		WorkersActive: promauto.With(registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "workers_active",
				Help:      "Number of currently active workers",
			},
			[]string{LabelWorkerID},
		),

		QueueDepth: promauto.With(registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "queue_depth",
				Help:      "Current number of jobs in queue by priority",
			},
			[]string{LabelPriority},
		),

		JobsInflight: promauto.With(registry).NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "jobs_inflight",
				Help:      "Number of jobs currently being processed",
			},
		),

		CronJobsActive: promauto.With(registry).NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "cron_jobs_active",
				Help:      "Number of active cron job schedules",
			},
		),

		SchedulerLoopDuration: promauto.With(registry).NewHistogram(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "scheduler_loop_duration_seconds",
				Help:      "Duration of scheduler main loop iteration",
				Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
			},
		),

		DelayedJobsMoved: promauto.With(registry).NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "delayed_jobs_moved_total",
				Help:      "Total delayed jobs moved to ready queue",
			},
		),

		RetryAttempts: promauto.With(registry).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "retry_attempts_total",
				Help:      "Total retry attempts by attempt number",
			},
			[]string{LabelJobType, LabelStatus},
		),

		CircuitBreakerState: promauto.With(registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "circuit_breaker_state",
				Help:      "Circuit breaker state (0=closed, 1=open, 2=half-open)",
			},
			[]string{LabelJobType},
		),
	}

	// Register Go runtime metrics
	registry.MustRegister(prometheus.NewGoCollector())
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	return c
}

// ---------------------------------------------------------------------------
// Recording Helpers
// ---------------------------------------------------------------------------

// RecordJobSubmitted increments the submitted counter.
func (c *Collector) RecordJobSubmitted(jobType string, priority int) {
	c.JobsSubmitted.WithLabelValues(jobType, fmt.Sprintf("%d", priority)).Inc()
}

// RecordJobExecuted increments the executed counter.
func (c *Collector) RecordJobExecuted(jobType, status string) {
	c.JobsExecuted.WithLabelValues(jobType, status).Inc()
}

// RecordJobFailed increments the failed counter.
func (c *Collector) RecordJobFailed(jobType, workerID string) {
	c.JobsFailed.WithLabelValues(jobType, workerID).Inc()
}

// RecordJobRetried increments the retried counter.
func (c *Collector) RecordJobRetried(jobType string) {
	c.JobsRetried.WithLabelValues(jobType).Inc()
}

// RecordJobDLQ increments the DLQ counter.
func (c *Collector) RecordJobDLQ(jobType string) {
	c.JobsDLQ.WithLabelValues(jobType).Inc()
}

// RecordJobCancelled increments the cancelled counter.
func (c *Collector) RecordJobCancelled(jobType string) {
	c.JobsCancelled.WithLabelValues(jobType).Inc()
}

// RecordJobLatency records the end-to-end job latency.
func (c *Collector) RecordJobLatency(jobType string, duration time.Duration) {
	c.JobLatency.WithLabelValues(jobType).Observe(duration.Seconds())
}

// RecordJobExecutionTime records job execution duration.
func (c *Collector) RecordJobExecutionTime(jobType string, duration time.Duration) {
	c.JobExecutionTime.WithLabelValues(jobType).Observe(duration.Seconds())
}

// RecordEnqueueLatency records the time taken to enqueue a job.
func (c *Collector) RecordEnqueueLatency(duration time.Duration) {
	c.EnqueueLatency.Observe(duration.Seconds())
}

// RecordDequeueLatency records the time taken to dequeue a job.
func (c *Collector) RecordDequeueLatency(duration time.Duration) {
	c.DequeueLatency.Observe(duration.Seconds())
}

// SetWorkerActive sets the active worker gauge for a worker.
func (c *Collector) SetWorkerActive(workerID string, active float64) {
	c.WorkersActive.WithLabelValues(workerID).Set(active)
}

// SetQueueDepth sets the queue depth gauge for a priority level.
func (c *Collector) SetQueueDepth(priority int, depth float64) {
	c.QueueDepth.WithLabelValues(fmt.Sprintf("%d", priority)).Set(depth)
}

// SetJobsInflight sets the in-flight jobs gauge.
func (c *Collector) SetJobsInflight(count float64) {
	c.JobsInflight.Set(count)
}

// SetCronJobsActive sets the active cron jobs gauge.
func (c *Collector) SetCronJobsActive(count float64) {
	c.CronJobsActive.Set(count)
}

// RecordSchedulerLoop records the scheduler loop duration.
func (c *Collector) RecordSchedulerLoop(duration time.Duration) {
	c.SchedulerLoopDuration.Observe(duration.Seconds())
}

// RecordDelayedJobsMoved increments the delayed jobs moved counter.
func (c *Collector) RecordDelayedJobsMoved(count int) {
	c.DelayedJobsMoved.Add(float64(count))
}

// RecordRetryAttempt records a retry attempt.
func (c *Collector) RecordRetryAttempt(jobType, status string) {
	c.RetryAttempts.WithLabelValues(jobType, status).Inc()
}

// SetCircuitBreakerState records the circuit breaker state.
func (c *Collector) SetCircuitBreakerState(jobType string, state float64) {
	c.CircuitBreakerState.WithLabelValues(jobType).Set(state)
}

// ---------------------------------------------------------------------------
// HTTP Handler
// ---------------------------------------------------------------------------

// HTTPHandler returns an http.Handler for the metrics endpoint.
func (c *Collector) HTTPHandler() http.Handler {
	return promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// ---------------------------------------------------------------------------
// WorkerActivityTracker
// ---------------------------------------------------------------------------

// WorkerActivityTracker tracks per-worker activity for reporting.
type WorkerActivityTracker struct {
	collector *Collector
	workerID  string
}

// NewWorkerTracker creates a tracker for a specific worker.
func (c *Collector) NewWorkerTracker(workerID string) *WorkerActivityTracker {
	return &WorkerActivityTracker{
		collector: c,
		workerID:  workerID,
	}
}

// RecordJobProcessed records a completed job for this worker.
func (w *WorkerActivityTracker) RecordJobProcessed(jobType string, success bool) {
	status := "success"
	if !success {
		status = "failure"
		w.collector.RecordJobFailed(jobType, w.workerID)
	}
	w.collector.RecordJobExecuted(jobType, status)
}

// SetActive updates the worker's active status.
func (w *WorkerActivityTracker) SetActive(active bool) {
	val := 0.0
	if active {
		val = 1.0
	}
	w.collector.SetWorkerActive(w.workerID, val)
}

// ---------------------------------------------------------------------------
// QueueDepthUpdater
// ---------------------------------------------------------------------------

// UpdateQueueDepth updates all priority queue depth gauges.
func (c *Collector) UpdateQueueDepth(depths map[int]int64) {
	for priority, count := range depths {
		c.SetQueueDepth(priority, float64(count))
	}
}