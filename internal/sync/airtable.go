package syncer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
// leave headroom for clock skew between client and server. The burst must stay
// at 1: a token bucket admits burst + rate requests in its first second, and a
// sync opens with the schema plus every table's first page at once, so any
// larger burst overshoots 5 req/s and earns a 30s 429 penalty on every run.
const (
	defaultBaseRateLimit = rate.Limit(4)
	defaultBaseRateBurst = 1

	// Airtable also throttles each signed-in user across all of their bases,
	// at about the same rate as one base. Budgeting only per base lets one
	// person with two bases syncing run at twice their limit, and every 429
	// then parks a table for 30 seconds or more. defaultUserRateLimit is the
	// total for one user. defaultUserBulkRateLimit caps how much of it large
	// downloads may take, so refreshes, writes and schema reads for the same
	// person always find room instead of queueing behind a download.
	defaultUserRateLimit     = rate.Limit(4)
	defaultUserBulkRateLimit = rate.Limit(3)

	// A download also stands aside while the same user's small requests are
	// arriving, so a refresh of many tables runs at the user's full rate
	// instead of the share the download leaves. bulkYieldWindow is how long
	// after a small request a download page holds off, and bulkYieldMaxWaits
	// bounds the holding off so a long run of small requests cannot stall a
	// download outright.
	bulkYieldWindow   = 300 * time.Millisecond
	bulkYieldMaxWaits = 20

	// 6 attempts is 5 retries. With Airtable's 30s Retry-After hint and the
	// exponential schedule below, the worst-case total wait per request is
	// bounded by maxRetryBackoff * (maxRetryAttempts-1).
	defaultMaxRetryAttempts = 6
	defaultMaxRetryBackoff  = 90 * time.Second

	// defaultHTTPTimeout bounds each Airtable request end-to-end (including
	// body read) so a stalled connection surfaces as a retriable error
	// instead of hanging a sync worker forever.
	defaultHTTPTimeout = 30 * time.Second
)

// APIError is a non-2xx, non-429 response from the Airtable API. Its message
// format is unchanged from the plain error it replaced; the type exists so
// callers can branch on the status code (notably 401 after a token rotation).
type APIError struct {
	Method     string
	Path       string
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("airtable API %s %s returned %d: %s", e.Method, e.Path, e.StatusCode, e.Body)
}

// IsUnauthorized reports whether err is an Airtable 401, which is what an
// access token gets once a refresh has rotated it out.
func IsUnauthorized(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusUnauthorized
}

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

	mu           sync.Mutex
	userLimiters map[string]*userLimiterEntry
	baseLimiters map[string]*baseLimiterEntry

	baseRateLimit     rate.Limit
	baseRateBurst     int
	userRateLimit     rate.Limit
	userBulkRateLimit rate.Limit
	maxRetryAttempts  int
	maxRetryBackoff   time.Duration
}

// userLimiterEntry is one signed-in user's Airtable budget. Every request
// draws from all; requests marked with WithBulkRequests draw from bulk first.
type userLimiterEntry struct {
	all  *rate.Limiter
	bulk *rate.Limiter
	// slowUntil is set after a 429: until then both limiters run at half rate.
	slowUntil time.Time
	// lastSmallAt is when the user's latest non-bulk request asked for a slot.
	lastSmallAt time.Time
	lastSeen    time.Time
}

type bulkRequestsKey struct{}

// WithBulkRequests marks the Airtable requests made with ctx as part of a
// large download. They are held to a share of the user's budget so that the
// same user's small requests are not stuck behind them.
func WithBulkRequests(ctx context.Context) context.Context {
	return context.WithValue(ctx, bulkRequestsKey{}, true)
}

func isBulkRequest(ctx context.Context) bool {
	bulk, _ := ctx.Value(bulkRequestsKey{}).(bool)
	return bulk
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
	ID             string  `json:"id"`
	Name           string  `json:"name"`
	Description    string  `json:"description,omitempty"`
	PrimaryFieldID string  `json:"primary_field_id,omitempty"`
	Fields         []Field `json:"fields"`
}

type Field struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
	// Options is Airtable's type-specific field configuration, kept verbatim.
	// Delta sync reads link, lookup and rollup wiring from it.
	Options json.RawMessage `json:"options,omitempty"`
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
		ID             string  `json:"id"`
		Name           string  `json:"name"`
		Description    string  `json:"description"`
		PrimaryFieldID string  `json:"primaryFieldId"`
		Fields         []Field `json:"fields"`
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
	// ModifiedSince, when set, limits the page to records created or edited
	// by a user after that instant.
	ModifiedSince time.Time
	// RecordIDs, when set, limits the page to those records. Records that no
	// longer exist are simply absent from the result.
	RecordIDs []string
}

// MaxRecordIDsPerRequest is how many record IDs one ListRecordsPage call may
// ask for: one full page.
const MaxRecordIDsPerRequest = 100

// modifiedSinceFormula matches records created or edited after since.
// LAST_MODIFIED_TIME() is blank for a record no user has edited, and a blank
// operand makes IS_AFTER an error that would drop the record, so those fall
// back to their creation time.
func modifiedSinceFormula(since time.Time) string {
	return fmt.Sprintf(
		"IS_AFTER(IF(LAST_MODIFIED_TIME(), LAST_MODIFIED_TIME(), CREATED_TIME()), '%s')",
		since.UTC().Format("2006-01-02T15:04:05.000Z"),
	)
}

func recordIDsFormula(ids []string) string {
	terms := make([]string, 0, len(ids))
	for _, id := range ids {
		terms = append(terms, "RECORD_ID()='"+strings.ReplaceAll(id, "'", "")+"'")
	}
	return "OR(" + strings.Join(terms, ",") + ")"
}

func (o ListRecordsPageOptions) filterFormula() string {
	var parts []string
	if !o.ModifiedSince.IsZero() {
		parts = append(parts, modifiedSinceFormula(o.ModifiedSince))
	}
	if len(o.RecordIDs) > 0 {
		parts = append(parts, recordIDsFormula(o.RecordIDs))
	}
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return parts[0]
	default:
		return "AND(" + strings.Join(parts, ",") + ")"
	}
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
		httpClient = &http.Client{
			Timeout: defaultHTTPTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		}
	}

	return &HTTPClient{
		baseURL:           strings.TrimRight(baseURL, "/"),
		httpClient:        httpClient,
		clock:             time.Now,
		sleep:             sleepContext,
		randomFloat:       rand.Float64,
		userLimiters:      make(map[string]*userLimiterEntry),
		baseLimiters:      make(map[string]*baseLimiterEntry),
		baseRateLimit:     defaultBaseRateLimit,
		baseRateBurst:     defaultBaseRateBurst,
		userRateLimit:     defaultUserRateLimit,
		userBulkRateLimit: defaultUserBulkRateLimit,
		maxRetryAttempts:  defaultMaxRetryAttempts,
		maxRetryBackoff:   defaultMaxRetryBackoff,
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
			ID:             table.ID,
			Name:           table.Name,
			Description:    table.Description,
			PrimaryFieldID: table.PrimaryFieldID,
			Fields:         table.Fields,
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
	recordPath := fmt.Sprintf("/v0/%s/%s", url.PathEscape(baseID), url.PathEscape(tableID))
	sortField := strings.TrimSpace(options.SortFieldName)
	direction := strings.ToLower(strings.TrimSpace(options.SortDirection))
	if direction == "" {
		direction = "desc"
	}

	var payload listRecordsResponse
	if formula := options.filterFormula(); formula != "" {
		// A filter over a hundred record IDs outgrows a URL, so filtered
		// pages go through the POST form of the same endpoint.
		body := map[string]any{"pageSize": 100, "filterByFormula": formula}
		if options.Offset != "" {
			body["offset"] = options.Offset
		}
		if sortField != "" {
			body["sort"] = []map[string]string{{"field": sortField, "direction": direction}}
		}
		if err := c.doJSON(ctx, accessToken, http.MethodPost, recordPath+"/listRecords", nil, body, &payload); err != nil {
			return ListRecordsPageResult{}, err
		}
		return ListRecordsPageResult(payload), nil
	}

	query := url.Values{}
	query.Set("pageSize", "100")
	if options.Offset != "" {
		query.Set("offset", options.Offset)
	}
	if sortField != "" {
		query.Set("sort[0][field]", sortField)
		query.Set("sort[0][direction]", direction)
	}
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
				c.slowUserAfterRateLimit(accessToken, retryDelay)
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
			return &APIError{
				Method:     method,
				Path:       request.URL.Path,
				StatusCode: response.StatusCode,
				Body:       strings.TrimSpace(string(body)),
			}
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

	now := c.clock()
	c.mu.Lock()
	entry, exists := c.userLimiters[tokenKey]
	if !exists {
		entry = &userLimiterEntry{
			all:  rate.NewLimiter(c.userRateLimit, 1),
			bulk: rate.NewLimiter(c.userBulkRateLimit, 1),
		}
		c.userLimiters[tokenKey] = entry
	}
	entry.lastSeen = now
	if !entry.slowUntil.IsZero() && !now.Before(entry.slowUntil) {
		entry.all.SetLimitAt(now, c.userRateLimit)
		entry.bulk.SetLimitAt(now, c.userBulkRateLimit)
		entry.slowUntil = time.Time{}
	}
	bulk := isBulkRequest(ctx)
	if !bulk {
		entry.lastSmallAt = now
	}
	c.pruneUserLimitersLocked(now)
	c.mu.Unlock()

	if bulk {
		if err := c.yieldToSmallRequests(ctx, entry); err != nil {
			return err
		}
		if err := c.waitForLimiter(ctx, entry.bulk, "user "+tokenKey[:8]+" bulk"); err != nil {
			return err
		}
	}
	return c.waitForLimiter(ctx, entry.all, "user "+tokenKey[:8])
}

// yieldToSmallRequests holds a download page back while the same user's small
// requests are still arriving.
func (c *HTTPClient) yieldToSmallRequests(ctx context.Context, entry *userLimiterEntry) error {
	for attempt := 0; attempt < bulkYieldMaxWaits; attempt++ {
		c.mu.Lock()
		lastSmallAt := entry.lastSmallAt
		c.mu.Unlock()
		if lastSmallAt.IsZero() {
			return nil
		}
		wait := bulkYieldWindow - c.clock().Sub(lastSmallAt)
		if wait <= 0 {
			return nil
		}
		if err := c.sleep(ctx, wait); err != nil {
			return err
		}
	}
	return nil
}

// slowUserAfterRateLimit halves a user's budget for as long as Airtable asked
// the rejected request to wait. A 429 means the user is over their limit in
// Airtable's eyes, whatever this process has counted: other clients spend the
// same allowance.
func (c *HTTPClient) slowUserAfterRateLimit(accessToken string, delay time.Duration) {
	tokenKey := rateLimitTokenKey(accessToken)
	if tokenKey == "" || delay <= 0 {
		return
	}
	now := c.clock()
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, exists := c.userLimiters[tokenKey]
	if !exists {
		return
	}
	if entry.slowUntil.IsZero() {
		entry.all.SetLimitAt(now, c.userRateLimit/2)
		entry.bulk.SetLimitAt(now, c.userBulkRateLimit/2)
	}
	if until := now.Add(delay); until.After(entry.slowUntil) {
		entry.slowUntil = until
	}
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

	return c.waitForLimiter(ctx, limiter, "base "+baseID)
}

// waitForLimiter takes one slot from limiter and sleeps until it is due.
func (c *HTTPClient) waitForLimiter(ctx context.Context, limiter *rate.Limiter, name string) error {
	now := c.clock()
	reservation := limiter.ReserveN(now, 1)
	if !reservation.OK() {
		return fmt.Errorf("airtable rate limiter for %s rejected reservation", name)
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

func (c *HTTPClient) pruneUserLimitersLocked(now time.Time) {
	const ttl = 10 * time.Minute
	for key, entry := range c.userLimiters {
		if now.Sub(entry.lastSeen) > ttl {
			delete(c.userLimiters, key)
		}
	}
}

func (c *HTTPClient) pruneBaseLimitersLocked(now time.Time) {
	const ttl = 10 * time.Minute
	for key, entry := range c.baseLimiters {
		if now.Sub(entry.lastSeen) > ttl {
			delete(c.baseLimiters, key)
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
