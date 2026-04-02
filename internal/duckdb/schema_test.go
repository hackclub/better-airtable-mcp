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
