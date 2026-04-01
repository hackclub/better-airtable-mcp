package tools

import "testing"

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
