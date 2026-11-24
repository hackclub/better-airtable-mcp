package syncer

import (
	"reflect"
	"testing"
)

func TestSchemaGraphWatchesOnlyTheFieldsAConditionalRollupReads(t *testing.T) {
	link := simOptions(map[string]any{"linkedTableId": "tblProjects"})
	tables := []Table{
		{ID: "tblAuthors", Name: "Authors", PrimaryFieldID: "fldAuthorName0001", Fields: []Field{
			{ID: "fldAuthorName0001", Name: "Name", Type: "singleLineText"},
			{ID: "fldAuthorLink0001", Name: "Projects", Type: "multipleRecordLinks", Options: link},
			{ID: "fldAuthorSum00001", Name: "Grants This Year", Type: "rollup", Options: simOptions(map[string]any{
				"recordLinkFieldId":    "fldAuthorLink0001",
				"fieldIdInLinkedTable": "fldProjGrant00001",
				"filters":              map[string]any{"operator": "and", "operands": []any{map[string]any{"operator": "isAfter", "operands": []any{"fldProjApproved01", "2026-01-01"}}}},
			})},
			{ID: "fldAuthorOdd00001", Name: "Odd", Type: "rollup", Options: simOptions(map[string]any{
				"recordLinkFieldId":    "fldAuthorLink0001",
				"fieldIdInLinkedTable": "fldProjGrant00001",
				"filters":              map[string]any{"operator": "contains", "operands": []any{"fldNotInSource001", "x"}},
			})},
		}},
		{ID: "tblProjects", Name: "Projects", PrimaryFieldID: "fldProjName000001", Fields: []Field{
			{ID: "fldProjName000001", Name: "Name", Type: "singleLineText"},
			{ID: "fldProjGrant00001", Name: "Grant", Type: "number"},
			{ID: "fldProjApproved01", Name: "Approved At", Type: "date"},
			{ID: "fldProjStars00001", Name: "Stars", Type: "number"},
		}},
	}
	plans, err := buildSyncPlans(tables)
	if err != nil {
		t.Fatalf("buildSyncPlans: %v", err)
	}

	// Without the second rollup, only the two fields the first one reads
	// matter: a change to Stars must not send every author to be refetched.
	trimmed := []Table{tables[0], tables[1]}
	trimmed[0].Fields = tables[0].Fields[:3]
	trimmedPlans, _ := buildSyncPlans(trimmed)
	columns, any := buildSchemaGraph(trimmed, trimmedPlans).watchFor("tblProjects")
	if any || !reflect.DeepEqual(columns, []string{"approved_at", "grant"}) {
		t.Fatalf("watch = %v (any %v), want [approved_at grant] and not any", columns, any)
	}

	// A condition on a field that is not a cached column of the source table
	// cannot be watched precisely, so any change counts.
	_, any = buildSchemaGraph(tables, plans).watchFor("tblProjects")
	if !any {
		t.Fatalf("expected a rollup with an unresolvable condition to watch the whole row")
	}
}
