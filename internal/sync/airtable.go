package syncer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/hackclub/better-airtable-mcp/internal/logx"
)

const defaultAPIBaseURL = "https://api.airtable.com"

// Airtable's documented per-base rate limit is 5 req/s. We run below that to
// leave headroom for clock skew between client and server, and we use a token
// bucket (rather than strict spacing) so brief bursts can drain the burst
// budget while sustained traffic settles to the steady-state rate.
const (
	defaultBaseRateLimit = rate.Limit(4)
	defaultBaseRateBurst = 4

	// 6 attempts is 5 retries. With Airtable's 30s Retry-After hint and the
	// exponential schedule below, the worst-case total wait per request is
	// bounded by maxRetryBackoff * (maxRetryAttempts-1).
	defaultMaxRetryAttempts = 6
	defaultMaxRetryBackoff  = 90 * time.Second
)

type Client interface {
	ListBases(ctx context.Context, accessToken string) ([]Base, error)
	GetBaseSchema(ctx context.Context, accessToken, baseID string) ([]Table, error)
	ListRecordsPage(ctx context.Context, accessToken, baseID, tableID string, options ListRecordsPageOptions) (ListRecordsPageResult, error)
	ListRecords(ctx context.Context, accessToken, baseID, tableID string) ([]Record, error)
}

type MutationRecord struct {
	ID     string         `json:"id,omitempty"`
	Fields map[string]any `json:"fields,omitempty"`
}

type HTTPClient struct {
	baseURL     string
	httpClient  *http.Client
	clock       func() time.Time
	sleep       func(context.Context, time.Duration) error
	randomFloat func() float64

	mu             sync.Mutex
	nextUserWindow map[string]*rateWindow
	baseLimiters   map[string]*baseLimiterEntry

	baseRateLimit    rate.Limit
	baseRateBurst    int
	maxRetryAttempts int
	maxRetryBackoff  time.Duration
}

type rateWindow struct {
	nextAllowedAt time.Time
	lastSeen      time.Time
}

type baseLimiterEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

type Base struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	PermissionLevel string `json:"permission_level"`
}

type Table struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Description string  `json:"description,omitempty"`
	Fields      []Field `json:"fields"`
}

type Field struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
}

type Record struct {
	ID          string         `json:"id"`
	CreatedTime time.Time      `json:"createdTime"`
	Fields      map[string]any `json:"fields"`
}

type listBasesResponse struct {
	Bases []struct {
		ID              string `json:"id"`
		Name            string `json:"name"`
		PermissionLevel string `json:"permissionLevel"`
	} `json:"bases"`
	Offset string `json:"offset"`
}

type baseSchemaResponse struct {
	Tables []struct {
		ID          string  `json:"id"`
		Name        string  `json:"name"`
		Description string  `json:"description"`
		Fields      []Field `json:"fields"`
	} `json:"tables"`
}

type listRecordsResponse struct {
	Records []Record `json:"records"`
	Offset  string   `json:"offset"`
}

type ListRecordsPageOptions struct {
	Offset        string
	SortFieldName string
	SortDirection string
}

type ListRecordsPageResult struct {
	Records []Record
	Offset  string
}

func NewHTTPClient(baseURL string, httpClient *http.Client) *HTTPClient {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultAPIBaseURL
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	return &HTTPClient{
		baseURL:          strings.TrimRight(baseURL, "/"),
		httpClient:       httpClient,
		clock:            time.Now,
		sleep:            sleepContext,
		randomFloat:      rand.Float64,
		nextUserWindow:   make(map[string]*rateWindow),
		baseLimiters:     make(map[string]*baseLimiterEntry),
		baseRateLimit:    defaultBaseRateLimit,
		baseRateBurst:    defaultBaseRateBurst,
		maxRetryAttempts: defaultMaxRetryAttempts,
		maxRetryBackoff:  defaultMaxRetryBackoff,
	}
}

func (c *HTTPClient) ListBases(ctx context.Context, accessToken string) ([]Base, error) {
	bases := make([]Base, 0)
	offset := ""

	// Airtable returns at most 1000 bases per page and hands back an "offset" to
	// fetch the next one. Follow it, or every base past #1000 silently vanishes.
	for {
		var query url.Values
		if offset != "" {
			query = url.Values{"offset": {offset}}
		}

		var payload listBasesResponse
		if err := c.doJSON(ctx, accessToken, http.MethodGet, "/v0/meta/bases", query, nil, &payload); err != nil {
			return nil, err
		}

		for _, base := range payload.Bases {
			bases = append(bases, Base{
				ID:              base.ID,
				Name:            base.Name,
				PermissionLevel: base.PermissionLevel,
			})
		}

		if payload.Offset == "" {
			break
		}
		offset = payload.Offset
	}

	return bases, nil
}

func (c *HTTPClient) GetBaseSchema(ctx context.Context, accessToken, baseID string) ([]Table, error) {
	var payload baseSchemaResponse
	if err := c.doJSON(ctx, accessToken, http.MethodGet, path.Join("/v0/meta/bases", baseID, "tables"), nil, nil, &payload); err != nil {
		return nil, err
	}

	tables := make([]Table, 0, len(payload.Tables))
	for _, table := range payload.Tables {
		tables = append(tables, Table{
			ID:          table.ID,
			Name:        table.Name,
			Description: table.Description,
			Fields:      table.Fields,
		})
	}

	return tables, nil
}

func (c *HTTPClient) ListRecords(ctx context.Context, accessToken, baseID, tableID string) ([]Record, error) {
	records := make([]Record, 0)
	offset := ""

	for {
		page, err := c.ListRecordsPage(ctx, accessToken, baseID, tableID, ListRecordsPageOptions{
			Offset: offset,
		})
		if err != nil {
			return nil, err
		}

		records = append(records, page.Records...)
		if page.Offset == "" {
			break
		}
		offset = page.Offset
	}

	return records, nil
}

func (c *HTTPClient) ListRecordsPage(ctx context.Context, accessToken, baseID, tableID string, options ListRecordsPageOptions) (ListRecordsPageResult, error) {
	query := url.Values{}
	query.Set("pageSize", "100")
	if options.Offset != "" {
		query.Set("offset", options.Offset)
	}
	if strings.TrimSpace(options.SortFieldName) != "" {
		query.Set("sort[0][field]", strings.TrimSpace(options.SortFieldName))
		direction := strings.ToLower(strings.TrimSpace(options.SortDirection))
		if direction == "" {
			direction = "desc"
		}
		query.Set("sort[0][direction]", direction)
	}

	var payload listRecordsResponse
	recordPath := fmt.Sprintf("/v0/%s/%s", url.PathEscape(baseID), url.PathEscape(tableID))
	if err := c.doJSON(ctx, accessToken, http.MethodGet, recordPath, query, nil, &payload); err != nil {
		return ListRecordsPageResult{}, err
	}

	return ListRecordsPageResult(payload), nil
}

func (c *HTTPClient) CreateRecords(ctx context.Context, accessToken, baseID, tableID string, records []MutationRecord) ([]Record, error) {
	var payload listRecordsResponse
	recordPath := fmt.Sprintf("/v0/%s/%s", url.PathEscape(baseID), url.PathEscape(tableID))
	if err := c.doJSON(ctx, accessToken, http.MethodPost, recordPath, nil, map[string]any{"records": records}, &payload); err != nil {
		return nil, err
	}
	return payload.Records, nil
}

func (c *HTTPClient) UpdateRecords(ctx context.Context, accessToken, baseID, tableID string, records []MutationRecord) ([]Record, error) {
	var payload listRecordsResponse
	recordPath := fmt.Sprintf("/v0/%s/%s", url.PathEscape(baseID), url.PathEscape(tableID))
	if err := c.doJSON(ctx, accessToken, http.MethodPatch, recordPath, nil, map[string]any{"records": records}, &payload); err != nil {
		return nil, err
	}
	return payload.Records, nil
}

func (c *HTTPClient) DeleteRecords(ctx context.Context, accessToken, baseID, tableID string, recordIDs []string) ([]string, error) {
	query := url.Values{}
	for _, recordID := range recordIDs {
		query.Add("records[]", recordID)
	}

	var payload struct {
		Records []struct {
			ID      string `json:"id"`
			Deleted bool   `json:"deleted"`
		} `json:"records"`
	}
	recordPath := fmt.Sprintf("/v0/%s/%s", url.PathEscape(baseID), url.PathEscape(tableID))
	if err := c.doJSON(ctx, accessToken, http.MethodDelete, recordPath, query, nil, &payload); err != nil {
		return nil, err
	}

	deleted := make([]string, 0, len(payload.Records))
	for _, record := range payload.Records {
		if record.Deleted {
			deleted = append(deleted, record.ID)
		}
	}
	return deleted, nil
}

// FieldDefinition describes a field to create on a table, either as part of a
// new table or added to an existing one. Options is the Airtable field-options
// object (choices for selects, linked table id for links, etc.) and is passed
// through verbatim; it is omitted for types that take no options.
type FieldDefinition struct {
	Name        string         `json:"name"`
	Type        string         `json:"type"`
	Description string         `json:"description,omitempty"`
	Options     map[string]any `json:"options,omitempty"`
}

// readOnlyCreateOptions lists field-options keys that Airtable returns when
// reading a field but rejects at creation time (INVALID_FIELD_TYPE_OPTIONS_FOR_CREATE),
// keyed by field type. For multipleRecordLinks, create accepts only linkedTableId
// (+ optional viewIdForRecordSelection); the rest are read-only.
var readOnlyCreateOptions = map[string]map[string]struct{}{
	"multipleRecordLinks": {
		"isReversed":              {},
		"prefersSingleRecordLink": {},
		"inverseLinkFieldId":      {},
	},
}

// sanitizeCreateOptions drops options that are read-only at field-creation time
// for the given field type, so callers can pass a field's full (read-shaped)
// options without tripping Airtable's create-only schema validation.
func sanitizeCreateOptions(fieldType string, options map[string]any) map[string]any {
	readOnly, ok := readOnlyCreateOptions[fieldType]
	if !ok || len(options) == 0 {
		return options
	}
	cleaned := make(map[string]any, len(options))
	for k, v := range options {
		if _, drop := readOnly[k]; drop {
			continue
		}
		cleaned[k] = v
	}
	return cleaned
}

// CreateTable creates a new table in a base via the meta API. Airtable requires
// at least one field, and the first field becomes the table's primary field.
func (c *HTTPClient) CreateTable(ctx context.Context, accessToken, baseID, name, description string, fields []FieldDefinition) (Table, error) {
	sanitized := make([]FieldDefinition, len(fields))
	for i, f := range fields {
		f.Options = sanitizeCreateOptions(f.Type, f.Options)
		sanitized[i] = f
	}
	body := map[string]any{
		"name":   name,
		"fields": sanitized,
	}
	if strings.TrimSpace(description) != "" {
		body["description"] = description
	}

	var table Table
	requestPath := path.Join("/v0/meta/bases", baseID, "tables")
	if err := c.doJSON(ctx, accessToken, http.MethodPost, requestPath, nil, body, &table); err != nil {
		return Table{}, err
	}
	return table, nil
}

// CreateField adds a field to an existing table via the meta API.
func (c *HTTPClient) CreateField(ctx context.Context, accessToken, baseID, tableID string, field FieldDefinition) (Field, error) {
	body := map[string]any{
		"name": field.Name,
		"type": field.Type,
	}
	if strings.TrimSpace(field.Description) != "" {
		body["description"] = field.Description
	}
	if opts := sanitizeCreateOptions(field.Type, field.Options); len(opts) > 0 {
		body["options"] = opts
	}

	var created Field
	requestPath := path.Join("/v0/meta/bases", baseID, "tables", tableID, "fields")
	if err := c.doJSON(ctx, accessToken, http.MethodPost, requestPath, nil, body, &created); err != nil {
		return Field{}, err
	}
	return created, nil
}

// UpdateTable renames a table and/or updates its description. Airtable's meta
// API only allows changing name and description, not structure.
func (c *HTTPClient) UpdateTable(ctx context.Context, accessToken, baseID, tableID, name, description string) (Table, error) {
	body := map[string]any{}
	if strings.TrimSpace(name) != "" {
		body["name"] = name
	}
	if strings.TrimSpace(description) != "" {
		body["description"] = description
	}

	var table Table
	requestPath := path.Join("/v0/meta/bases", baseID, "tables", tableID)
	if err := c.doJSON(ctx, accessToken, http.MethodPatch, requestPath, nil, body, &table); err != nil {
		return Table{}, err
	}
	return table, nil
}

// UpdateField renames a field and/or updates its description. Airtable's meta
// API only allows changing name and description, never the field's type.
func (c *HTTPClient) UpdateField(ctx context.Context, accessToken, baseID, tableID, fieldID, name, description string) (Field, error) {
	body := map[string]any{}
	if strings.TrimSpace(name) != "" {
		body["name"] = name
	}
	if strings.TrimSpace(description) != "" {
		body["description"] = description
	}

	var field Field
	requestPath := path.Join("/v0/meta/bases", baseID, "tables", tableID, "fields", fieldID)
	if err := c.doJSON(ctx, accessToken, http.MethodPatch, requestPath, nil, body, &field); err != nil {
		return Field{}, err
	}
	return field, nil
}

func (c *HTTPClient) doJSON(ctx context.Context, accessToken, method, requestPath string, query url.Values, body any, target any) error {
	var encodedBody []byte
	if body != nil {
		var err error
		encodedBody, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode airtable request body: %w", err)
		}
	}

	baseID, _ := airtableBaseIDFromPath(requestPath)
	tableID, _ := airtableTableIDFromPath(requestPath)
	maxAttempts := c.maxRetryAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := c.waitForRateLimits(ctx, accessToken, requestPath); err != nil {
			return err
		}

		endpoint := c.baseURL + requestPath
		if len(query) > 0 {
			endpoint += "?" + query.Encode()
		}

		var requestBody io.Reader
		if len(encodedBody) > 0 {
			requestBody = bytes.NewReader(encodedBody)
		}

		request, err := http.NewRequestWithContext(ctx, method, endpoint, requestBody)
		if err != nil {
			return fmt.Errorf("create airtable request: %w", err)
		}

		request.Header.Set("Authorization", "Bearer "+accessToken)
		request.Header.Set("Accept", "application/json")
		if body != nil {
			request.Header.Set("Content-Type", "application/json")
		}

		startedAt := c.clock()
		response, err := c.httpClient.Do(request)
		if err != nil {
			logx.Event(ctx, "airtable_client", "airtable.request.failed",
				"method", method,
				"base_id", baseID,
				"table_id", tableID,
				"endpoint_kind", airtableEndpointKind(requestPath),
				"attempt", attempt+1,
				"duration_ms", c.clock().Sub(startedAt).Milliseconds(),
				"error_kind", logx.ErrorKind(err),
				"error_message", logx.ErrorPreview(err),
			)
			return fmt.Errorf("perform airtable request: %w", err)
		}
		duration := c.clock().Sub(startedAt)

		if response.StatusCode == http.StatusTooManyRequests {
			response.Body.Close()
			if attempt < maxAttempts-1 {
				retryDelay := c.computeRateLimitBackoff(response.Header.Get("Retry-After"), attempt)
				logx.Event(ctx, "airtable_client", "airtable.request.retry",
					"method", method,
					"base_id", baseID,
					"table_id", tableID,
					"endpoint_kind", airtableEndpointKind(requestPath),
					"attempt", attempt+1,
					"status", response.StatusCode,
					"duration_ms", duration.Milliseconds(),
					"retry_delay_ms", retryDelay.Milliseconds(),
				)
				if err := c.sleep(ctx, retryDelay); err != nil {
					return err
				}
				continue
			}
			break
		}

		defer response.Body.Close()

		if response.StatusCode < 200 || response.StatusCode >= 300 {
			body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			logx.Event(ctx, "airtable_client", "airtable.request.failed",
				"method", method,
				"base_id", baseID,
				"table_id", tableID,
				"endpoint_kind", airtableEndpointKind(requestPath),
				"attempt", attempt+1,
				"status", response.StatusCode,
				"duration_ms", duration.Milliseconds(),
				"error_kind", "external_api",
				"error_message", logx.SanitizeExternalBody(string(body)),
			)
			return fmt.Errorf("airtable API %s %s returned %d: %s", method, request.URL.Path, response.StatusCode, strings.TrimSpace(string(body)))
		}

		if target == nil {
			if duration >= logx.AirtableSlowRequestThreshold {
				logx.Event(ctx, "airtable_client", "airtable.request.completed",
					"method", method,
					"base_id", baseID,
					"table_id", tableID,
					"endpoint_kind", airtableEndpointKind(requestPath),
					"attempt", attempt+1,
					"status", response.StatusCode,
					"duration_ms", duration.Milliseconds(),
					"slow", true,
				)
			}
			return nil
		}

		if err := json.NewDecoder(response.Body).Decode(target); err != nil {
			logx.Event(ctx, "airtable_client", "airtable.request.failed",
				"method", method,
				"base_id", baseID,
				"table_id", tableID,
				"endpoint_kind", airtableEndpointKind(requestPath),
				"attempt", attempt+1,
				"status", response.StatusCode,
				"duration_ms", duration.Milliseconds(),
				"error_kind", logx.ErrorKind(err),
				"error_message", logx.ErrorPreview(err),
			)
			return fmt.Errorf("decode airtable response: %w", err)
		}
		if duration >= logx.AirtableSlowRequestThreshold {
			logx.Event(ctx, "airtable_client", "airtable.request.completed",
				"method", method,
				"base_id", baseID,
				"table_id", tableID,
				"endpoint_kind", airtableEndpointKind(requestPath),
				"attempt", attempt+1,
				"status", response.StatusCode,
				"duration_ms", duration.Milliseconds(),
				"slow", true,
			)
		}

		return nil
	}

	logx.Event(ctx, "airtable_client", "airtable.request.failed",
		"method", method,
		"base_id", baseID,
		"table_id", tableID,
		"endpoint_kind", airtableEndpointKind(requestPath),
		"attempt", maxAttempts,
		"error_kind", "rate_limit",
		"error_message", "airtable API returned repeated rate limits",
	)
	return fmt.Errorf("airtable API %s %s returned repeated rate limits after %d attempts", method, requestPath, maxAttempts)
}

func (c *HTTPClient) computeRateLimitBackoff(header string, attempt int) time.Duration {
	delay := retryAfterDelay(header)

	if attempt > 0 {
		multiplier := math.Pow(1.5, float64(attempt))
		scaled := time.Duration(float64(delay) * multiplier)
		if scaled > delay {
			delay = scaled
		}
	}

	if c.randomFloat != nil && delay > 0 {
		jitter := time.Duration(c.randomFloat() * 0.3 * float64(delay))
		delay += jitter
	}

	if c.maxRetryBackoff > 0 && delay > c.maxRetryBackoff {
		delay = c.maxRetryBackoff
	}
	if delay < 0 {
		delay = 0
	}
	return delay
}

func (c *HTTPClient) waitForRateLimits(ctx context.Context, accessToken, requestPath string) error {
	if err := c.waitForUserRateLimit(ctx, accessToken); err != nil {
		return err
	}
	return c.waitForBaseRateLimit(ctx, requestPath)
}

func (c *HTTPClient) waitForUserRateLimit(ctx context.Context, accessToken string) error {
	tokenKey := rateLimitTokenKey(accessToken)
	if tokenKey == "" {
		return nil
	}
	return c.waitForWindow(ctx, c.nextUserWindow, tokenKey, 20*time.Millisecond)
}

func (c *HTTPClient) waitForBaseRateLimit(ctx context.Context, requestPath string) error {
	baseID, ok := airtableBaseIDFromPath(requestPath)
	if !ok {
		return nil
	}

	now := c.clock()

	c.mu.Lock()
	entry, exists := c.baseLimiters[baseID]
	if !exists {
		entry = &baseLimiterEntry{
			limiter: rate.NewLimiter(c.baseRateLimit, c.baseRateBurst),
		}
		c.baseLimiters[baseID] = entry
	}
	entry.lastSeen = now
	c.pruneBaseLimitersLocked(now)
	limiter := entry.limiter
	c.mu.Unlock()

	reservation := limiter.ReserveN(now, 1)
	if !reservation.OK() {
		return fmt.Errorf("airtable rate limiter for base %s rejected reservation", baseID)
	}
	delay := reservation.DelayFrom(now)
	if delay <= 0 {
		return nil
	}
	if err := c.sleep(ctx, delay); err != nil {
		reservation.CancelAt(c.clock())
		return err
	}
	return nil
}

func (c *HTTPClient) pruneBaseLimitersLocked(now time.Time) {
	const ttl = 10 * time.Minute
	for key, entry := range c.baseLimiters {
		if now.Sub(entry.lastSeen) > ttl {
			delete(c.baseLimiters, key)
		}
	}
}

func (c *HTTPClient) waitForWindow(ctx context.Context, windows map[string]*rateWindow, key string, interval time.Duration) error {
	if strings.TrimSpace(key) == "" {
		return nil
	}

	c.mu.Lock()
	now := c.clock()
	entry, ok := windows[key]
	if !ok {
		entry = &rateWindow{}
		windows[key] = entry
	}
	nextAllowedAt := entry.nextAllowedAt
	if nextAllowedAt.Before(now) {
		nextAllowedAt = now
	}
	entry.nextAllowedAt = nextAllowedAt.Add(interval)
	entry.lastSeen = now
	c.pruneRateWindowsLocked(now)
	c.mu.Unlock()

	if delay := nextAllowedAt.Sub(now); delay > 0 {
		return c.sleep(ctx, delay)
	}
	return nil
}

func (c *HTTPClient) pruneRateWindowsLocked(now time.Time) {
	const ttl = 10 * time.Minute
	for key, window := range c.nextUserWindow {
		if now.Sub(window.lastSeen) > ttl {
			delete(c.nextUserWindow, key)
		}
	}
}

func airtableBaseIDFromPath(requestPath string) (string, bool) {
	parts := strings.Split(strings.Trim(requestPath, "/"), "/")
	if len(parts) == 0 || parts[0] != "v0" {
		return "", false
	}

	if len(parts) >= 4 && parts[1] == "meta" && parts[2] == "bases" {
		return parts[3], true
	}
	if len(parts) >= 2 && parts[1] != "meta" {
		return parts[1], true
	}

	return "", false
}

func airtableTableIDFromPath(requestPath string) (string, bool) {
	parts := strings.Split(strings.Trim(requestPath, "/"), "/")
	if len(parts) >= 3 && parts[0] == "v0" && parts[1] != "meta" {
		return parts[2], true
	}
	return "", false
}

func airtableEndpointKind(requestPath string) string {
	if strings.HasPrefix(requestPath, "/v0/meta/bases/") && strings.HasSuffix(requestPath, "/tables") {
		return "base_schema"
	}
	if requestPath == "/v0/meta/bases" {
		return "list_bases"
	}
	if strings.HasPrefix(requestPath, "/v0/meta/bases/") {
		if strings.Contains(requestPath, "/fields") {
			return "field_meta"
		}
		return "table_meta"
	}
	if _, ok := airtableTableIDFromPath(requestPath); ok {
		return "records"
	}
	return "unknown"
}

func retryAfterDelay(header string) time.Duration {
	if header == "" {
		return 30 * time.Second
	}
	if seconds, err := time.ParseDuration(strings.TrimSpace(header) + "s"); err == nil {
		return seconds
	}
	if parsed, err := http.ParseTime(header); err == nil {
		if delay := time.Until(parsed); delay > 0 {
			return delay
		}
	}
	return 30 * time.Second
}

func rateLimitTokenKey(accessToken string) string {
	accessToken = strings.TrimSpace(accessToken)
	if accessToken == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(accessToken))
	return hex.EncodeToString(sum[:])
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
