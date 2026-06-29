// Package worker provides job handlers that implement the actual business logic
// for each job type. This file includes the handler interface, registry,
// and example handlers for common job types.
//
// Adding a new handler:
//   1. Create a struct that implements JobHandler
//   2. Register it in cmd/worker/main.go
//   3. Handle the context cancellation for graceful shutdown
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/rajeshwarrao1253/distributed-task-scheduler/internal/models"
)

// =============================================================================
// JobHandler Interface
// =============================================================================

// JobHandler is the interface that all job handlers must implement.
// Each handler is responsible for executing a specific type of job.
type JobHandler interface {
	// Name returns the unique job type identifier this handler handles.
	// Example: "send-email", "process-payment".
	Name() string

	// Execute processes the given job. The context includes a timeout
	// and will be cancelled if the job exceeds its configured timeout.
	// Implementations must respect context cancellation for graceful shutdown.
	//
	// Returns nil on success, or an error that will trigger retry logic.
	Execute(ctx context.Context, job *models.Job) error
}

// =============================================================================
// Registry
// =============================================================================

// Registry maintains a mapping of job types to their handlers.
type Registry struct {
	handlers map[string]JobHandler
	logger   *zap.Logger
}

// NewRegistry creates a new handler registry.
func NewRegistry(logger *zap.Logger) *Registry {
	return &Registry{
		handlers: make(map[string]JobHandler),
		logger:   logger.With(zap.String("component", "handler_registry")),
	}
}

// Register adds a handler to the registry.
func (r *Registry) Register(handler JobHandler) {
	if handler == nil {
		r.logger.Warn("attempted to register nil handler")
		return
	}
	name := handler.Name()
	if existing, ok := r.handlers[name]; ok {
		r.logger.Warn("handler already registered, overwriting",
			zap.String("type", name),
			zap.Any("old", existing),
		)
	}
	r.handlers[name] = handler
	r.logger.Info("handler registered", zap.String("type", name))
}

// Get retrieves a handler by job type.
func (r *Registry) Get(jobType string) (JobHandler, bool) {
	h, ok := r.handlers[jobType]
	return h, ok
}

// RegisteredTypes returns all registered handler types.
func (r *Registry) RegisteredTypes() []string {
	types := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	return types
}

// =============================================================================
// Example: Send Email Handler
// =============================================================================

// SendEmailPayload defines the payload for the send-email job.
type SendEmailPayload struct {
	To       string `json:"to"`
	Subject  string `json:"subject"`
	Body     string `json:"body"`
	From     string `json:"from,omitempty"`
	CC       []string `json:"cc,omitempty"`
	BCC      []string `json:"bcc,omitempty"`
}

// SendEmailHandler handles sending email notifications.
type SendEmailHandler struct {
	smtpHost string
	smtpPort int
	logger   *zap.Logger
}

// NewSendEmailHandler creates a new email handler.
func NewSendEmailHandler(logger *zap.Logger) *SendEmailHandler {
	return &SendEmailHandler{
		smtpHost: "smtp.example.com",
		smtpPort: 587,
		logger:   logger.With(zap.String("handler", "send-email")),
	}
}

// Name returns the handler type.
func (h *SendEmailHandler) Name() string {
	return "send-email"
}

// Execute sends an email based on the job payload.
func (h *SendEmailHandler) Execute(ctx context.Context, job *models.Job) error {
	var payload SendEmailPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	h.logger.Debug("sending email",
		zap.String("to", payload.To),
		zap.String("subject", payload.Subject),
	)

	// Simulate email sending with context awareness
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(100 * time.Millisecond):
		// In production, this would use smtp.SendMail or an email service API
		h.logger.Info("email sent",
			zap.String("to", payload.To),
			zap.String("subject", payload.Subject),
		)
		return nil
	}
}

// =============================================================================
// Example: Process Payment Handler
// =============================================================================

// PaymentPayload defines the payload for payment processing.
type PaymentPayload struct {
	OrderID       string  `json:"order_id"`
	Amount        float64 `json:"amount"`
	Currency      string  `json:"currency"`
	PaymentMethod string  `json:"payment_method"`
	CustomerID    string  `json:"customer_id"`
}

// ProcessPaymentHandler handles payment processing with idempotency.
type ProcessPaymentHandler struct {
	paymentAPI string
	logger     *zap.Logger
}

// NewProcessPaymentHandler creates a new payment handler.
func NewProcessPaymentHandler(logger *zap.Logger) *ProcessPaymentHandler {
	return &ProcessPaymentHandler{
		paymentAPI: "https://api.payment-provider.com/v1/charges",
		logger:     logger.With(zap.String("handler", "process-payment")),
	}
}

// Name returns the handler type.
func (h *ProcessPaymentHandler) Name() string {
	return "process-payment"
}

// Execute processes a payment with idempotency key support.
func (h *ProcessPaymentHandler) Execute(ctx context.Context, job *models.Job) error {
	var payload PaymentPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	h.logger.Debug("processing payment",
		zap.String("order_id", payload.OrderID),
		zap.Float64("amount", payload.Amount),
	)

	// Validate payment data
	if payload.Amount <= 0 {
		return fmt.Errorf("invalid payment amount: %.2f", payload.Amount)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(200 * time.Millisecond):
		// In production, this would call the payment provider API
		// with idempotency key = job.ID to ensure exactly-once processing
		h.logger.Info("payment processed",
			zap.String("order_id", payload.OrderID),
			zap.Float64("amount", payload.Amount),
		)
		return nil
	}
}

// =============================================================================
// Example: Generate Report Handler
// =============================================================================

// ReportPayload defines the payload for report generation.
type ReportPayload struct {
	ReportType string            `json:"report_type"`
	Format     string            `json:"format,omitempty"`
	DateRange  struct {
		Start string `json:"start"`
		End   string `json:"end"`
	} `json:"date_range,omitempty"`
	Filters map[string]string `json:"filters,omitempty"`
	Recipients []string       `json:"recipients,omitempty"`
}

// GenerateReportHandler handles report generation.
type GenerateReportHandler struct {
	reportService string
	logger        *zap.Logger
}

// NewGenerateReportHandler creates a new report handler.
func NewGenerateReportHandler(logger *zap.Logger) *GenerateReportHandler {
	return &GenerateReportHandler{
		reportService: "https://internal-reports.api",
		logger:        logger.With(zap.String("handler", "generate-report")),
	}
}

// Name returns the handler type.
func (h *GenerateReportHandler) Name() string {
	return "generate-report"
}

// Execute generates a report based on the job payload.
func (h *GenerateReportHandler) Execute(ctx context.Context, job *models.Job) error {
	var payload ReportPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	// Default format
	if payload.Format == "" {
		payload.Format = "pdf"
	}

	h.logger.Debug("generating report",
		zap.String("type", payload.ReportType),
		zap.String("format", payload.Format),
	)

	// Simulate report generation - this could take a while
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(500 * time.Millisecond):
		h.logger.Info("report generated",
			zap.String("type", payload.ReportType),
			zap.String("format", payload.Format),
		)
		return nil
	}
}

// =============================================================================
// Example: Webhook Call Handler
// =============================================================================

// WebhookPayload defines the payload for webhook calls.
type WebhookPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    json.RawMessage   `json:"body,omitempty"`
	RetryPolicy struct {
		MaxRetries int `json:"max_retries,omitempty"`
		Timeout    int `json:"timeout_ms,omitempty"`
	} `json:"retry_policy,omitempty"`
}

// WebhookCallHandler handles outgoing webhook HTTP calls.
type WebhookCallHandler struct {
	httpClient *http.Client
	logger     *zap.Logger
}

// NewWebhookCallHandler creates a new webhook handler.
func NewWebhookCallHandler(logger *zap.Logger) *WebhookCallHandler {
	return &WebhookCallHandler{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		logger: logger.With(zap.String("handler", "webhook-call")),
	}
}

// Name returns the handler type.
func (h *WebhookCallHandler) Name() string {
	return "webhook-call"
}

// Execute makes an HTTP webhook call based on the job payload.
func (h *WebhookCallHandler) Execute(ctx context.Context, job *models.Job) error {
	var payload WebhookPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	// Validate URL
	if payload.URL == "" {
		return fmt.Errorf("webhook URL is required")
	}

	method := payload.Method
	if method == "" {
		method = "POST"
	}

	h.logger.Debug("making webhook call",
		zap.String("url", payload.URL),
		zap.String("method", method),
	)

	// Build request
	var bodyReader io.Reader
	if len(payload.Body) > 0 {
		bodyReader = bytes.NewReader(payload.Body)
	}

	req, err := http.NewRequestWithContext(ctx, method, payload.URL, bodyReader)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Webhook-ID", job.ID.String())
	for k, v := range payload.Headers {
		req.Header.Set(k, v)
	}

	// Execute request
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("webhook request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("webhook returned status %d: %s", resp.StatusCode, string(body))
	}

	h.logger.Info("webhook call succeeded",
		zap.String("url", payload.URL),
		zap.Int("status", resp.StatusCode),
	)

	return nil
}

// =============================================================================
// Example: Data Cleanup Handler
// =============================================================================

// DataCleanupPayload defines the payload for data cleanup jobs.
type DataCleanupPayload struct {
	Table     string `json:"table"`
	OlderThan string `json:"older_than"` // Duration string, e.g., "720h"
	BatchSize int    `json:"batch_size,omitempty"`
}

// DataCleanupHandler handles periodic data cleanup tasks.
type DataCleanupHandler struct {
	logger *zap.Logger
}

// NewDataCleanupHandler creates a new cleanup handler.
func NewDataCleanupHandler(logger *zap.Logger) *DataCleanupHandler {
	return &DataCleanupHandler{
		logger: logger.With(zap.String("handler", "data-cleanup")),
	}
}

// Name returns the handler type.
func (h *DataCleanupHandler) Name() string {
	return "data-cleanup"
}

// Execute performs data cleanup based on the job payload.
func (h *DataCleanupHandler) Execute(ctx context.Context, job *models.Job) error {
	var payload DataCleanupPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	if payload.Table == "" {
		return fmt.Errorf("table name is required")
	}

	batchSize := payload.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	h.logger.Debug("running data cleanup",
		zap.String("table", payload.Table),
		zap.String("older_than", payload.OlderThan),
		zap.Int("batch_size", batchSize),
	)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(300 * time.Millisecond):
		h.logger.Info("data cleanup completed",
			zap.String("table", payload.Table),
		)
		return nil
	}
}