package approval

import (
	"testing"

	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
)

func TestResolveTableSchemaAcceptsDuckDBOriginalAndAirtableIdentifiers(t *testing.T) {
	tables := []duckdb.TableSchema{
		{
			AirtableTableID: "tblProjects",
			DuckDBTableName: "table_1",
			OriginalName:    "Table 1",
		},
	}

	for _, requested := range []string{"table_1", "Table 1", "tblProjects", "table 1"} {
		resolved, aliases, ok := resolveTableSchema(tables, requested)
		if !ok {
			t.Fatalf("resolveTableSchema(%q) did not resolve", requested)
		}
		if resolved.DuckDBTableName != "table_1" {
			t.Fatalf("resolveTableSchema(%q) returned wrong table %#v", requested, resolved)
		}
		if len(aliases) != 3 {
			t.Fatalf("expected aliases for all table identifiers, got %#v", aliases)
		}
	}
}

func TestResolveFieldSchemaAcceptsDuckDBOriginalAndAirtableIdentifiers(t *testing.T) {
	fields := []duckdb.FieldSchema{
		{
			AirtableFieldID:  "fldName",
			DuckDBColumnName: "name",
			OriginalName:     "Name",
		},
		{
			AirtableFieldID:  "fldStatus",
			DuckDBColumnName: "status",
			OriginalName:     "Status",
		},
	}

	for _, requested := range []string{"name", "Name", "fldName", " status ", "Status", "fldStatus"} {
		resolved, ok := resolveFieldSchema(fields, requested)
		if !ok {
			t.Fatalf("resolveFieldSchema(%q) did not resolve", requested)
		}
		if requested == " status " || requested == "Status" || requested == "fldStatus" {
			if resolved.DuckDBColumnName != "status" {
				t.Fatalf("resolveFieldSchema(%q) returned wrong field %#v", requested, resolved)
			}
			continue
		}
		if resolved.DuckDBColumnName != "name" {
			t.Fatalf("resolveFieldSchema(%q) returned wrong field %#v", requested, resolved)
		}
	}
}

func TestCollectFieldAliasesDeduplicatesVariants(t *testing.T) {
	fields := []duckdb.FieldSchema{
		{
			AirtableFieldID:  "fldName",
			DuckDBColumnName: "name",
			OriginalName:     "Name",
		},
		{
			AirtableFieldID:  "fldNotes",
			DuckDBColumnName: "notes",
			OriginalName:     "Notes",
		},
	}

	aliases := collectFieldAliases(fields)
	expected := []string{"name", "fldName", "notes", "fldNotes"}
	if len(aliases) != len(expected) {
		t.Fatalf("expected %d aliases, got %#v", len(expected), aliases)
	}
	for index, want := range expected {
		if aliases[index] != want {
			t.Fatalf("expected alias %q at index %d, got %#v", want, index, aliases)
		}
	}
}
