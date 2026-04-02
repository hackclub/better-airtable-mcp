package tools

import "testing"

func TestNormalizeQueryAppliesDefaultLimit(t *testing.T) {
	normalized, err := NormalizeQuery("SELECT id, name FROM projects", 100, 1000)
	if err != nil {
		t.Fatalf("NormalizeQuery() returned error: %v", err)
	}

	if normalized.EffectiveLimit != 100 {
		t.Fatalf("expected effective limit 100, got %d", normalized.EffectiveLimit)
	}
	if normalized.SQL != "SELECT id, name FROM projects LIMIT 100" {
		t.Fatalf("unexpected normalized SQL %q", normalized.SQL)
	}
	if normalized.ExecutionSQL != "SELECT id, name FROM projects LIMIT 101" {
		t.Fatalf("unexpected execution SQL %q", normalized.ExecutionSQL)
	}
	if !normalized.ServerAppliedLimit {
		t.Fatal("expected NormalizeQuery() to report a server-applied limit")
	}
}

func TestNormalizeQueryPreservesExistingLimit(t *testing.T) {
	normalized, err := NormalizeQuery("SELECT * FROM projects LIMIT 5;", 100, 1000)
	if err != nil {
		t.Fatalf("NormalizeQuery() returned error: %v", err)
	}

	if normalized.SQL != "SELECT * FROM projects LIMIT 5;" {
		t.Fatalf("unexpected normalized SQL %q", normalized.SQL)
	}
	if normalized.ExecutionSQL != "SELECT * FROM projects LIMIT 5;" {
		t.Fatalf("unexpected execution SQL %q", normalized.ExecutionSQL)
	}
	if normalized.EffectiveLimit != 100 {
		t.Fatalf("expected effective limit 100, got %d", normalized.EffectiveLimit)
	}
	if normalized.ServerAppliedLimit {
		t.Fatal("expected existing SQL LIMIT to prevent a server-applied limit")
	}
}

func TestValidateReadOnlySQLRejectsDangerousStatements(t *testing.T) {
	testCases := []string{
		"DELETE FROM projects",
		"SELECT * FROM projects; DROP TABLE users",
		"WITH deleted AS (DELETE FROM tasks RETURNING *) SELECT * FROM deleted",
		"PRAGMA show_tables",
	}

	for _, testCase := range testCases {
		if err := ValidateReadOnlySQL(testCase); err == nil {
			t.Fatalf("expected %q to be rejected", testCase)
		}
	}
}

func TestValidateReadOnlySQLAllowsStringsAndComments(t *testing.T) {
	sql := `
		-- harmless semicolon in a comment ;
		SELECT *
		FROM projects
		WHERE note = 'hello; world'
	`

	if err := ValidateReadOnlySQL(sql); err != nil {
		t.Fatalf("expected SQL to be valid, got %v", err)
	}
}
