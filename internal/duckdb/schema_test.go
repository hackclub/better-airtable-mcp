package duckdb

import "testing"

func TestSanitizeIdentifier(t *testing.T) {
	testCases := []struct {
		name string
		want string
	}{
		{name: "Project Tracker 🚀", want: "project_tracker"},
		{name: "Q1 2026 OKRs", want: "q1_2026_okrs"},
		{name: "Tasks", want: "tasks"},
		{name: "!!!", want: "t_"},
		{name: "Status / Phase", want: "status_phase"},
	}

	for _, testCase := range testCases {
		if got := SanitizeIdentifier(testCase.name); got != testCase.want {
			t.Fatalf("SanitizeIdentifier(%q) = %q, want %q", testCase.name, got, testCase.want)
		}
	}
}

func TestSanitizeIdentifiersHandlesCollisions(t *testing.T) {
	got := SanitizeIdentifiers([]string{"Tasks", "tasks", "Tasks!!!"})
	want := []string{"tasks", "tasks_2", "tasks_3"}

	for index := range want {
		if got[index].Sanitized != want[index] {
			t.Fatalf("SanitizeIdentifiers()[%d] = %q, want %q", index, got[index].Sanitized, want[index])
		}
	}
}

func TestSanitizeFieldIdentifiersReservesImplicitColumns(t *testing.T) {
	got := SanitizeFieldIdentifiers([]string{"ID", "Created Time", "id", "created_time"})
	want := []string{"_airtable_id", "_airtable_created_time", "_airtable_id_2", "_airtable_created_time_2"}

	for index := range want {
		if got[index].Sanitized != want[index] {
			t.Fatalf("SanitizeFieldIdentifiers()[%d] = %q, want %q", index, got[index].Sanitized, want[index])
		}
	}
}

func TestAirtableTypeToDuckDBType(t *testing.T) {
	mapping, ok := AirtableTypeToDuckDBType("multipleRecordLinks")
	if !ok {
		t.Fatal("expected multipleRecordLinks to be supported")
	}
	if mapping.DuckDBType != "VARCHAR[]" {
		t.Fatalf("expected multipleRecordLinks to map to VARCHAR[], got %q", mapping.DuckDBType)
	}
	if mapping.Omitted {
		t.Fatal("expected multipleRecordLinks not to be omitted")
	}

	buttonMapping, ok := AirtableTypeToDuckDBType("button")
	if !ok {
		t.Fatal("expected button to be supported")
	}
	if !buttonMapping.Omitted {
		t.Fatal("expected button fields to be omitted")
	}
}

func TestSanitizeIdentifiersAvoidsGeneratedNameCollision(t *testing.T) {
	results := SanitizeIdentifiers([]string{"a b", "a-b", "a_b_2"})
	want := []string{"a_b", "a_b_2", "a_b_2_2"}
	for i, result := range results {
		if result.Sanitized != want[i] {
			t.Fatalf("results[%d].Sanitized = %q, want %q (all: %#v)", i, result.Sanitized, want[i], results)
		}
	}
}

func TestSanitizeIdentifiersAlwaysUnique(t *testing.T) {
	testCases := [][]string{
		{"x", "x", "x_2", "x_2"},
		{"a b", "a-b", "a_b_2", "a_b"},
		{"id", "ID", "_airtable_id", "id_2"},
		{"t", "t_", "t__", "t_2"},
	}
	for _, names := range testCases {
		results := SanitizeIdentifiers(names)
		unique := make(map[string]struct{}, len(results))
		for _, result := range results {
			unique[result.Sanitized] = struct{}{}
		}
		if len(unique) != len(names) {
			t.Fatalf("SanitizeIdentifiers(%q) produced duplicates: %#v", names, results)
		}
	}
}
