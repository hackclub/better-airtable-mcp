package tools

import (
	"encoding/json"
	"testing"
)

func TestValidateMutateInputAcceptsValidPayload(t *testing.T) {
	err := validateMutateInput(MutateInput{
		Base: "Project Tracker",
		Operations: []MutationOperation{
			{
				Type:  "update_records",
				Table: "projects",
				Records: []MutationRecord{
					{
						ID: "rec123",
						Fields: map[string]any{
							"name": "Updated Project",
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("validateMutateInput() returned error: %v", err)
	}
}

func TestValidateMutateInputRejectsCreateIDs(t *testing.T) {
	err := validateMutateInput(MutateInput{
		Base: "Project Tracker",
		Operations: []MutationOperation{
			{
				Type:  "create_records",
				Table: "projects",
				Records: []MutationRecord{
					{
						ID: "rec123",
						Fields: map[string]any{
							"name": "New Project",
						},
					},
				},
			},
		},
	})
	if err == nil {
		t.Fatal("expected create_records with IDs to be rejected")
	}
}

func TestValidateMutateInputRejectsDeleteFields(t *testing.T) {
	err := validateMutateInput(MutateInput{
		Base: "Project Tracker",
		Operations: []MutationOperation{
			{
				Type:  "delete_records",
				Table: "projects",
				Records: []MutationRecord{
					{
						ID: "rec123",
						Fields: map[string]any{
							"name": "Should not be present",
						},
					},
				},
			},
		},
	})
	if err == nil {
		t.Fatal("expected delete_records with fields to be rejected")
	}
}

func TestDecodeMutateInputAcceptsDeleteRecordIDStrings(t *testing.T) {
	var input MutateInput
	err := decodeArgs(json.RawMessage(`{
		"base": "app123",
		"operations": [
			{
				"type": "delete_records",
				"table": "Table 1",
				"records": ["rec1", "rec2"]
			}
		]
	}`), &input)
	if err != nil {
		t.Fatalf("decodeArgs() returned error: %v", err)
	}

	if len(input.Operations) != 1 || len(input.Operations[0].Records) != 2 {
		t.Fatalf("unexpected decoded operations %#v", input.Operations)
	}
	if input.Operations[0].Records[0].ID != "rec1" || input.Operations[0].Records[1].ID != "rec2" {
		t.Fatalf("expected delete record IDs to decode into MutationRecord IDs, got %#v", input.Operations[0].Records)
	}

	if err := validateMutateInput(input); err != nil {
		t.Fatalf("validateMutateInput() returned error: %v", err)
	}
}

func TestDecodeMutateInputPreservesDeleteObjectCompatibility(t *testing.T) {
	var input MutateInput
	err := decodeArgs(json.RawMessage(`{
		"base": "app123",
		"operations": [
			{
				"type": "delete_records",
				"table": "Table 1",
				"records": [{"id": "rec1"}, {"id": "rec2"}]
			}
		]
	}`), &input)
	if err != nil {
		t.Fatalf("decodeArgs() returned error: %v", err)
	}

	if len(input.Operations) != 1 || len(input.Operations[0].Records) != 2 {
		t.Fatalf("unexpected decoded operations %#v", input.Operations)
	}
	if input.Operations[0].Records[0].ID != "rec1" || input.Operations[0].Records[1].ID != "rec2" {
		t.Fatalf("expected delete object records to remain valid, got %#v", input.Operations[0].Records)
	}
}
