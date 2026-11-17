package tools

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hackclub/better-airtable-mcp/internal/duckdb"
)

func schemaTestBase() duckdb.BaseSchema {
	return duckdb.BaseSchema{
		BaseID:        "appProjects",
		BaseName:      "Project Tracker",
		DetailsLoaded: true,
		Tables: []duckdb.TableSchema{
			{
				AirtableTableID:  "tblProjects",
				DuckDBTableName:  "projects",
				OriginalName:     "Projects",
				Description:      "Every shipped project",
				TotalRecordCount: 200,
				TableComplete:    true,
				Fields: []duckdb.FieldSchema{
					{AirtableFieldID: "fldName", DuckDBColumnName: "name", OriginalName: "Name", Type: "VARCHAR", AirtableType: "singleLineText", IsPrimary: true},
					{AirtableFieldID: "fldStatus", DuckDBColumnName: "status", OriginalName: "Status", Type: "VARCHAR", AirtableType: "singleSelect", Description: "Review\tstate\nof the project", Options: `{"choices":[{"name":"Approved"},{"name":"Rejected"}]}`},
					{AirtableFieldID: "fldTasks", DuckDBColumnName: "linked_tasks", OriginalName: "Linked Tasks", Type: "VARCHAR[]", AirtableType: "multipleRecordLinks", Options: `{"linkedTableId":"tblTasks"}`},
					{AirtableFieldID: "fldTaskHours", DuckDBColumnName: "task_hours", OriginalName: "Task Hours", Type: "JSON", AirtableType: "rollup", Options: `{"recordLinkFieldId":"fldTasks","result":{"type":"number"}}`},
					{AirtableFieldID: "fldTaskNames", DuckDBColumnName: "task_names", OriginalName: "Task Names", Type: "JSON", AirtableType: "multipleLookupValues", Options: `{"recordLinkFieldId":"fldTasks"}`},
					{AirtableFieldID: "fldTaskCount", DuckDBColumnName: "task_count", OriginalName: "Task Count", Type: "BIGINT", AirtableType: "count", Options: `{"recordLinkFieldId":"fldTasks"}`},
					{AirtableFieldID: "fldLabel", DuckDBColumnName: "label", OriginalName: "Label", Type: "VARCHAR", AirtableType: "formula", Options: `{"result":{"type":"singleLineText"}}`},
				},
			},
			{
				AirtableTableID:  "tblTasks",
				DuckDBTableName:  "tasks",
				OriginalName:     "Tasks",
				TotalRecordCount: 40,
				TableComplete:    false,
				Fields: []duckdb.FieldSchema{
					{AirtableFieldID: "fldTaskName", DuckDBColumnName: "name", OriginalName: "Name", Type: "VARCHAR", AirtableType: "singleLineText", IsPrimary: true},
					{AirtableFieldID: "fldHours", DuckDBColumnName: "hours_spent", OriginalName: "Hours Spent", Type: "DOUBLE", AirtableType: "number"},
				},
			},
		},
	}
}

func schemaTestStats() map[string]duckdb.TableStats {
	return map[string]duckdb.TableStats{
		"projects": {Rows: 200, Columns: map[string]duckdb.ColumnStats{
			"name":         {Filled: 200, Example: "Website Redesign"},
			"status":       {Filled: 1, Example: "Approved"},
			"linked_tasks": {Filled: 199, Example: "[recTask1, recTask2]"},
			"task_hours":   {Filled: 0},
			"label":        {Filled: 100, Example: "line one\nline two\t" + strings.Repeat("x", 100)},
		}},
		"tasks": {Rows: 40, Columns: map[string]duckdb.ColumnStats{
			"name":        {Filled: 40, Example: "Design homepage"},
			"hours_spent": {Filled: 10, Example: "2.5"},
		}},
	}
}

func TestFormatSchemaDetailDescribesEveryColumn(t *testing.T) {
	schema := schemaTestBase()
	text := formatSchemaDetail(schema, schema.Tables, schemaTestStats(), 0)

	for _, want := range []string{
		"base: Project Tracker (appProjects), 2 tables, 9 columns\n",
		"# projects: 200 rows, 7 columns (Airtable table \"Projects\", tblProjects)\nEvery shipped project\n",
		"column\ttype\tairtable\tfilled\texample\tdescription\n",
		"name\tVARCHAR\tsingleLineText, primary\t100%\tWebsite Redesign\t\n",
		// One filled row out of 200 is not rounded down to 0%.
		"status\tVARCHAR\tsingleSelect: Approved | Rejected\t1%\tApproved\tReview state of the project\n",
		// 199 of 200 is not rounded up to 100%.
		"linked_tasks\tVARCHAR[]\tlink to tasks\t99%\t[recTask1, recTask2]\t\n",
		"task_hours\tJSON\trollup via linked_tasks (number), read-only\t0%\t\t\n",
		"task_names\tJSON\tlookup via linked_tasks, read-only\t\t\t\n",
		"task_count\tBIGINT\tcount of linked_tasks, read-only\t\t\t\n",
		"# tasks: 40 rows so far (still downloading), 2 columns (Airtable table \"Tasks\", tblTasks)\n",
		// No description column when no field of the table has one.
		"column\ttype\tairtable\tfilled\texample\n",
		"hours_spent\tDOUBLE\tnumber\t25%\t2.5\n",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected detail to contain %q, got:\n%s", want, text)
		}
	}

	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "label\t") {
			continue
		}
		cells := strings.Split(line, "\t")
		if len(cells) != 6 {
			t.Fatalf("an example with tabs and newlines broke the row into %d cells: %q", len(cells), line)
		}
		if example := cells[4]; len([]rune(example)) != schemaExampleMaxChars || !strings.HasPrefix(example, "line one line two xxx") || !strings.HasSuffix(example, "...") {
			t.Fatalf("unexpected example cell %q", example)
		}
	}
}

func TestFormatSchemaOverviewListsTablesWithLinks(t *testing.T) {
	text := formatSchemaOverview(schemaTestBase())
	for _, want := range []string{
		"base: Project Tracker (appProjects), 2 tables, 9 columns\n",
		"Call list_schema again with tables",
		"table\trows\tcolumns\tprimary\tlinks_to\tdescription\n",
		"projects\t200\t7\tname\ttasks\tEvery shipped project\n",
		"tasks\t40+\t2\tname\t\t\n",
		"A row count ending in + is still downloading.",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected overview to contain %q, got:\n%s", want, text)
		}
	}
	if strings.Contains(text, "hours_spent") {
		t.Fatalf("the overview must not list columns, got:\n%s", text)
	}
}

func TestFormatSchemaDetailStopsAtTheBudget(t *testing.T) {
	schema := schemaTestBase()
	wide := duckdb.TableSchema{AirtableTableID: "tblWide", DuckDBTableName: "wide", OriginalName: "Wide", TableComplete: true}
	for index := 0; index < 400; index++ {
		wide.Fields = append(wide.Fields, duckdb.FieldSchema{
			AirtableFieldID:  fmt.Sprintf("fld%d", index),
			DuckDBColumnName: fmt.Sprintf("column_number_%03d", index),
			Type:             "VARCHAR",
			AirtableType:     "singleLineText",
		})
	}

	// A first table that alone exceeds the budget is cut by columns.
	text := formatSchemaDetail(schema, []duckdb.TableSchema{wide, schema.Tables[1]}, nil, 4000)
	if len(text) > 4400 {
		t.Fatalf("expected the detail to stay near its 4000 character budget, got %d", len(text))
	}
	if !strings.Contains(text, "column_number_000\t") || strings.Contains(text, "column_number_399\t") {
		t.Fatalf("expected the first columns only, got:\n%s", text)
	}
	if !strings.Contains(text, "of 400 columns of wide. Use search to find the others.") {
		t.Fatalf("expected a note about the cut columns, got:\n%s", text)
	}
	if !strings.Contains(text, "Not shown, to keep this result small: tasks.") {
		t.Fatalf("expected the second table to be named as left out, got:\n%s", text)
	}

	// Later tables that do not fit are left out whole.
	text = formatSchemaDetail(schema, []duckdb.TableSchema{schema.Tables[1], wide}, nil, 4000)
	if !strings.Contains(text, "hours_spent\t") || strings.Contains(text, "column_number_000\t") || !strings.Contains(text, "Not shown, to keep this result small: wide.") {
		t.Fatalf("expected tasks in full and wide left out, got:\n%s", text)
	}
}

func TestSearchSchemaColumnsMatchesAnyTermInNamesAndDescriptions(t *testing.T) {
	schema := schemaTestBase()

	terms := schemaSearchTerms(" Hours,  REVIEW hours ")
	if strings.Join(terms, "|") != "hours|review" {
		t.Fatalf("unexpected terms %#v", terms)
	}
	matches := searchSchemaColumns(schema.Tables, terms)
	got := make([]string, 0, len(matches))
	for _, match := range matches {
		got = append(got, match.Table.DuckDBTableName+"."+match.Field.DuckDBColumnName)
	}
	// status matches through its description, hours_spent through its
	// Airtable name as well as its column name.
	if strings.Join(got, " ") != "projects.status projects.task_hours tasks.hours_spent" {
		t.Fatalf("unexpected matches %v", got)
	}

	text := formatSchemaSearch(schema, schema.Tables, terms, matches, schemaTestStats())
	for _, want := range []string{
		"3 columns match any of: hours, review.",
		"table\tcolumn\ttype\tairtable\tfilled\texample\tdescription\n",
		"projects\ttask_hours\tJSON\trollup via linked_tasks (number), read-only\t0%\t\t\n",
		"tasks\thours_spent\tDOUBLE\tnumber\t25%\t2.5\t\n",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected search result to contain %q, got:\n%s", want, text)
		}
	}

	none := formatSchemaSearch(schema, schema.Tables, []string{"zzz"}, nil, nil)
	if !strings.Contains(none, "No column name or description contains any of: zzz.") || !strings.Contains(none, "Tables searched: projects, tasks") {
		t.Fatalf("unexpected empty search result:\n%s", none)
	}
}

func TestDescribeChoicesCapsLongChoiceLists(t *testing.T) {
	var options airtableFieldOptions
	for index := 0; index < 45; index++ {
		options.Choices = append(options.Choices, struct {
			Name string `json:"name"`
		}{Name: fmt.Sprintf("Choice %d", index)})
	}
	text := describeChoices(options)
	if !strings.HasPrefix(text, "Choice 0 | Choice 1 | ") || !strings.HasSuffix(text, fmt.Sprintf("(+%d more)", 45-schemaMaxChoicesShown)) {
		t.Fatalf("unexpected choices text %q", text)
	}
}

func TestFillShareKeepsZeroAndFullExact(t *testing.T) {
	cases := []struct {
		filled, rows int64
		want         string
	}{
		{0, 0, ""}, {0, 10, "0%"}, {10, 10, "100%"}, {1, 100000, "1%"}, {99999, 100000, "99%"}, {1, 2, "50%"}, {2, 3, "67%"},
	}
	for _, tc := range cases {
		if got := fillShare(tc.filled, tc.rows); got != tc.want {
			t.Fatalf("fillShare(%d, %d) = %q, want %q", tc.filled, tc.rows, got, tc.want)
		}
	}
}
