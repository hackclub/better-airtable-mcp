package syncer

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"
)

// rotatingTokenClient mimics Airtable revoking an access token the moment the
// refresh loop rotates it: once a request arrives after the rotation point,
// only the newest token is accepted and every older one gets a 401.
type rotatingTokenClient struct {
	mu           sync.Mutex
	currentToken string
	pageCalls    int
	tokensSeen   []string
}

func (c *rotatingTokenClient) rotate(newToken string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.currentToken = newToken
}

func (c *rotatingTokenClient) token() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currentToken
}

func (c *rotatingTokenClient) check(accessToken string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tokensSeen = append(c.tokensSeen, accessToken)
	if accessToken != c.currentToken {
		return &APIError{Method: http.MethodGet, Path: "/v0/appRotate/tblRecords", StatusCode: http.StatusUnauthorized, Body: `{"error":{"type":"UNAUTHORIZED","message":"Invalid authentication token"}}`}
	}
	return nil
}

func (c *rotatingTokenClient) ListBases(ctx context.Context, accessToken string) ([]Base, error) {
	return []Base{{ID: "appRotate", Name: "Rotate"}}, nil
}

func (c *rotatingTokenClient) GetBaseSchema(ctx context.Context, accessToken, baseID string) ([]Table, error) {
	if err := c.check(accessToken); err != nil {
		return nil, err
	}
	return []Table{{
		ID:     "tblRecords",
		Name:   "Records",
		Fields: []Field{{ID: "fldName", Name: "Name", Type: "singleLineText"}},
	}}, nil
}

func (c *rotatingTokenClient) ListRecordsPage(ctx context.Context, accessToken, baseID, tableID string, options ListRecordsPageOptions) (ListRecordsPageResult, error) {
	c.mu.Lock()
	c.pageCalls++
	call := c.pageCalls
	c.mu.Unlock()

	// The second page request is in flight with the old token at the moment
	// the refresh loop rotates it.
	if call == 2 {
		c.rotate("token-2")
	}
	if err := c.check(accessToken); err != nil {
		return ListRecordsPageResult{}, err
	}

	created := time.Date(2026, 10, 13, 14, 0, 0, 0, time.UTC)
	switch options.Offset {
	case "":
		return ListRecordsPageResult{
			Records: []Record{{ID: "rec1", CreatedTime: created, Fields: map[string]any{"Name": "first"}}},
			Offset:  "page2",
		}, nil
	case "page2":
		return ListRecordsPageResult{
			Records: []Record{{ID: "rec2", CreatedTime: created, Fields: map[string]any{"Name": "second"}}},
		}, nil
	}
	return ListRecordsPageResult{}, nil
}

func (c *rotatingTokenClient) ListRecords(ctx context.Context, accessToken, baseID, tableID string) ([]Record, error) {
	return nil, nil
}

func TestRunSyncSurvivesTokenRotationMidRun(t *testing.T) {
	client := &rotatingTokenClient{currentToken: "token-1"}
	service := NewService(client, t.TempDir())

	// The token source reads whatever the token store holds right now, the
	// way TokenManager.AirtableAccessToken does after a refresh.
	tokens := func(context.Context) (string, error) { return client.token(), nil }

	result, err := service.runSync(context.Background(), tokens, Base{ID: "appRotate", Name: "Rotate"}, nil)
	if err != nil {
		t.Fatalf("runSync failed across a token rotation: %v", err)
	}
	if result.RecordsSynced != 2 {
		t.Fatalf("expected 2 records synced, got %d", result.RecordsSynced)
	}

	queried, err := service.QueryResolvedBase(context.Background(), "appRotate", "SELECT count(*) AS n FROM records")
	if err != nil {
		t.Fatalf("query synced base: %v", err)
	}
	if got := queried.Rows[0][0]; got != int64(2) {
		t.Fatalf("expected 2 rows in the cache, got %#v", got)
	}
}

func TestRunSyncWithCapturedTokenFailsAfterRotation(t *testing.T) {
	// The pre-fix behaviour: one token captured at the start of the run. The
	// rotation mid-run turns every later page into a 401 and fails the sync.
	client := &rotatingTokenClient{currentToken: "token-1"}
	service := NewService(client, t.TempDir())

	_, err := service.runSync(context.Background(), StaticAccessToken("token-1"), Base{ID: "appRotate", Name: "Rotate"}, nil)
	if err == nil || !IsUnauthorized(err) {
		t.Fatalf("expected a 401 with a captured token, got %v", err)
	}
}
