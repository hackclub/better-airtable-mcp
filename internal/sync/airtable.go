package syncer

import (
	"bytes"
	"context"
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

	mu             sync.Mutex
	nextBaseWindow map[string]time.Time
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
		nextBaseWindow: make(map[string]time.Time),
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
		query := url.Values{}
		query.Set("pageSize", "100")
		if offset != "" {
			query.Set("offset", offset)
		}

		var payload listRecordsResponse
		recordPath := fmt.Sprintf("/v0/%s/%s", url.PathEscape(baseID), url.PathEscape(tableID))
		if err := c.doJSON(ctx, accessToken, http.MethodGet, recordPath, query, nil, &payload); err != nil {
			return nil, err
		}

		records = append(records, payload.Records...)
		if payload.Offset == "" {
			break
		}
		offset = payload.Offset
	}

	return records, nil
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
		if err := c.waitForBaseRateLimit(ctx, requestPath); err != nil {
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
			if err := sleepContext(ctx, retryDelay); err != nil {
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

func (c *HTTPClient) waitForBaseRateLimit(ctx context.Context, requestPath string) error {
	baseID, ok := airtableBaseIDFromPath(requestPath)
	if !ok {
		return nil
	}

	c.mu.Lock()
	now := c.clock()
	nextAllowedAt := c.nextBaseWindow[baseID]
	if nextAllowedAt.Before(now) {
		nextAllowedAt = now
	}
	c.nextBaseWindow[baseID] = nextAllowedAt.Add(200 * time.Millisecond)
	c.mu.Unlock()

	if delay := nextAllowedAt.Sub(now); delay > 0 {
		return sleepContext(ctx, delay)
	}
	return nil
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
