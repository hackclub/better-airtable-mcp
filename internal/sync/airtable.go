package syncer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"
)

const defaultAPIBaseURL = "https://api.airtable.com"

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
	baseURL    string
	httpClient *http.Client
	clock      func() time.Time
	sleep      func(context.Context, time.Duration) error

	mu             sync.Mutex
	nextBaseWindow map[string]*rateWindow
	nextUserWindow map[string]*rateWindow
}

type rateWindow struct {
	nextAllowedAt time.Time
	lastSeen      time.Time
}

type Base struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	PermissionLevel string `json:"permission_level"`
}

type Table struct {
	ID     string  `json:"id"`
	Name   string  `json:"name"`
	Fields []Field `json:"fields"`
}

type Field struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
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
}

type baseSchemaResponse struct {
	Tables []struct {
		ID     string  `json:"id"`
		Name   string  `json:"name"`
		Fields []Field `json:"fields"`
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
		baseURL:        strings.TrimRight(baseURL, "/"),
		httpClient:     httpClient,
		clock:          time.Now,
		sleep:          sleepContext,
		nextBaseWindow: make(map[string]*rateWindow),
		nextUserWindow: make(map[string]*rateWindow),
	}
}

func (c *HTTPClient) ListBases(ctx context.Context, accessToken string) ([]Base, error) {
	var payload listBasesResponse
	if err := c.doJSON(ctx, accessToken, http.MethodGet, "/v0/meta/bases", nil, nil, &payload); err != nil {
		return nil, err
	}

	bases := make([]Base, 0, len(payload.Bases))
	for _, base := range payload.Bases {
		bases = append(bases, Base{
			ID:              base.ID,
			Name:            base.Name,
			PermissionLevel: base.PermissionLevel,
		})
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
			ID:     table.ID,
			Name:   table.Name,
			Fields: table.Fields,
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

	return ListRecordsPageResult{
		Records: payload.Records,
		Offset:  payload.Offset,
	}, nil
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

func (c *HTTPClient) doJSON(ctx context.Context, accessToken, method, requestPath string, query url.Values, body any, target any) error {
	var encodedBody []byte
	if body != nil {
		var err error
		encodedBody, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode airtable request body: %w", err)
		}
	}

	for attempt := 0; attempt < 2; attempt++ {
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

		response, err := c.httpClient.Do(request)
		if err != nil {
			return fmt.Errorf("perform airtable request: %w", err)
		}

		if response.StatusCode == http.StatusTooManyRequests && attempt == 0 {
			retryDelay := retryAfterDelay(response.Header.Get("Retry-After"))
			response.Body.Close()
			if err := c.sleep(ctx, retryDelay); err != nil {
				return err
			}
			continue
		}

		defer response.Body.Close()

		if response.StatusCode < 200 || response.StatusCode >= 300 {
			body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
			return fmt.Errorf("airtable API %s %s returned %d: %s", method, request.URL.Path, response.StatusCode, strings.TrimSpace(string(body)))
		}

		if target == nil {
			return nil
		}

		if err := json.NewDecoder(response.Body).Decode(target); err != nil {
			return fmt.Errorf("decode airtable response: %w", err)
		}

		return nil
	}

	return fmt.Errorf("airtable API %s %s returned repeated rate limits", method, requestPath)
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

	return c.waitForWindow(ctx, c.nextBaseWindow, baseID, 200*time.Millisecond)
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
	for key, window := range c.nextBaseWindow {
		if now.Sub(window.lastSeen) > ttl {
			delete(c.nextBaseWindow, key)
		}
	}
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
