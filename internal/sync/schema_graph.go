package syncer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

// fieldWiring is the part of a field's Airtable options that says which other
// fields its value is computed from.
type fieldWiring struct {
	LinkedTableID        string          `json:"linkedTableId"`
	RecordLinkFieldID    string          `json:"recordLinkFieldId"`
	FieldIDInLinkedTable string          `json:"fieldIdInLinkedTable"`
	ReferencedFieldIDs   []string        `json:"referencedFieldIds"`
	Filters              json.RawMessage `json:"filters"`
	Sorts                json.RawMessage `json:"sorts"`
	RecordLimit          json.RawMessage `json:"recordLimit"`
}

func (f Field) wiring() fieldWiring {
	var wiring fieldWiring
	if len(f.Options) > 0 {
		_ = json.Unmarshal(f.Options, &wiring)
	}
	return wiring
}

func rawPresent(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null"
}

// computedEdge says that rows of DependentTable hold values computed from
// rows of SourceTable, reached through DependentTable's link column
// LinkColumn. Airtable's modified time ignores computed fields, so when a
// source row changes the dependent rows must be refetched explicitly.
type computedEdge struct {
	DependentTableID string
	LinkColumn       string // DuckDB column of the link field in the dependent table
	SourceTableID    string
	// SourceColumn is the DuckDB column in the source table whose change
	// matters. Empty means any change to the source row matters.
	SourceColumn string
}

// schemaGraph is the cross-record dependency wiring of one base.
type schemaGraph struct {
	edgesBySource map[string][]computedEdge
	// linkColumns lists, per table, its link columns and the table each one
	// points at.
	linkColumns map[string]map[string]string
}

func buildSchemaGraph(tables []Table, plans []syncTablePlan) schemaGraph {
	graph := schemaGraph{
		edgesBySource: map[string][]computedEdge{},
		linkColumns:   map[string]map[string]string{},
	}

	columnByField := map[string]map[string]string{}
	for _, plan := range plans {
		columns := map[string]string{}
		for _, field := range plan.Table.Fields {
			columns[field.AirtableFieldID] = field.DuckDBColumnName
		}
		columnByField[plan.Table.AirtableTableID] = columns
	}
	primaryField := map[string]string{}
	for _, table := range tables {
		primaryField[table.ID] = table.PrimaryFieldID
	}

	seen := map[computedEdge]struct{}{}
	addEdge := func(edge computedEdge) {
		if _, ok := seen[edge]; ok {
			return
		}
		seen[edge] = struct{}{}
		graph.edgesBySource[edge.SourceTableID] = append(graph.edgesBySource[edge.SourceTableID], edge)
	}

	for _, table := range tables {
		linkTargets := map[string]string{} // link field ID -> linked table ID
		for _, field := range table.Fields {
			if field.Type != "multipleRecordLinks" {
				continue
			}
			wiring := field.wiring()
			column := columnByField[table.ID][field.ID]
			if wiring.LinkedTableID == "" || column == "" {
				continue
			}
			linkTargets[field.ID] = wiring.LinkedTableID
			if graph.linkColumns[table.ID] == nil {
				graph.linkColumns[table.ID] = map[string]string{}
			}
			graph.linkColumns[table.ID][column] = wiring.LinkedTableID
		}

		for _, field := range table.Fields {
			wiring := field.wiring()
			switch field.Type {
			case "multipleLookupValues", "lookup", "rollup", "count":
				sourceTable, ok := linkTargets[wiring.RecordLinkFieldID]
				if !ok {
					continue
				}
				linkColumn := columnByField[table.ID][wiring.RecordLinkFieldID]
				conditional := rawPresent(wiring.Filters) || rawPresent(wiring.Sorts) || rawPresent(wiring.RecordLimit)
				sourceColumn := columnByField[sourceTable][wiring.FieldIDInLinkedTable]
				if field.Type == "count" && !conditional {
					// A plain count only depends on which records are linked,
					// and a link edit already marks this row as modified.
					continue
				}
				if conditional || sourceColumn == "" {
					sourceColumn = ""
				}
				addEdge(computedEdge{DependentTableID: table.ID, LinkColumn: linkColumn, SourceTableID: sourceTable, SourceColumn: sourceColumn})
			}
			// A formula (or rollup aggregation) that names a link field
			// renders the linked records' primary field.
			if field.Type == "formula" || field.Type == "rollup" {
				for _, referenced := range wiring.ReferencedFieldIDs {
					sourceTable, ok := linkTargets[referenced]
					if !ok {
						continue
					}
					addEdge(computedEdge{
						DependentTableID: table.ID,
						LinkColumn:       columnByField[table.ID][referenced],
						SourceTableID:    sourceTable,
						SourceColumn:     columnByField[sourceTable][primaryField[sourceTable]],
					})
				}
			}
		}
	}
	return graph
}

// watchFor reports what a delta write to a source table has to compare so
// the edges out of it can be followed.
func (g schemaGraph) watchFor(tableID string) (columns []string, any bool) {
	seen := map[string]struct{}{}
	for _, edge := range g.edgesBySource[tableID] {
		if edge.SourceColumn == "" {
			any = true
			continue
		}
		if _, ok := seen[edge.SourceColumn]; !ok {
			seen[edge.SourceColumn] = struct{}{}
			columns = append(columns, edge.SourceColumn)
		}
	}
	sort.Strings(columns)
	return columns, any
}

func (g schemaGraph) linkColumnNames(tableID string) []string {
	names := make([]string, 0, len(g.linkColumns[tableID]))
	for name := range g.linkColumns[tableID] {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// schemaHash fingerprints everything about a base's schema that decides the
// cache's layout or the values already stored in it. Views, descriptions and
// select choices are left out: they change often and never invalidate rows.
func schemaHash(tables []Table) string {
	type hashedField struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		Type    string `json:"type"`
		Options any    `json:"options,omitempty"`
	}
	type hashedTable struct {
		ID      string        `json:"id"`
		Name    string        `json:"name"`
		Primary string        `json:"primary"`
		Fields  []hashedField `json:"fields"`
	}

	hashed := make([]hashedTable, 0, len(tables))
	for _, table := range tables {
		entry := hashedTable{ID: table.ID, Name: table.Name, Primary: table.PrimaryFieldID}
		for _, field := range table.Fields {
			var options any
			if len(field.Options) > 0 {
				if err := json.Unmarshal(field.Options, &options); err == nil {
					options = stripChoices(options)
				}
			}
			entry.Fields = append(entry.Fields, hashedField{ID: field.ID, Name: field.Name, Type: field.Type, Options: options})
		}
		hashed = append(hashed, entry)
	}
	encoded, _ := json.Marshal(hashed)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func stripChoices(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, inner := range typed {
			if key == "choices" {
				continue
			}
			out[key] = stripChoices(inner)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, inner := range typed {
			out = append(out, stripChoices(inner))
		}
		return out
	default:
		return value
	}
}
