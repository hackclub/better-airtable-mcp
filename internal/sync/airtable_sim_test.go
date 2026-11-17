package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

// simAirtable is an in-memory stand-in for Airtable with the behaviours delta
// sync depends on, as observed against a real base:
//   - a record's modified time moves only when a user-editable field changes
//     and is blank until the first edit;
//   - editing a link edits the inverse link on the other record too, so both
//     ends count as modified;
//   - deleting a record removes it from every link that pointed at it and
//     marks those records as modified;
//   - lookups, rollups, counts and formulas are recomputed on read and never
//     move the modified time.
type simAirtable struct {
	mu       sync.Mutex
	clock    time.Time
	pageSize int
	nextID   int
	tables   map[string]*simTable
	order    []string

	schemaCalls int
	pageCalls   int
	extraField  bool // adds a field to the schema, to simulate a schema change
}

type simTable struct {
	id, name string
	records  map[string]*simRecord
}

type simRecord struct {
	id       string
	created  time.Time
	modified time.Time // zero until a user edits the record
	text     map[string]string
	number   map[string]float64
	links    map[string][]string
}

const (
	simProjects = "tblProjects"
	simPrograms = "tblPrograms"
	simOrgs     = "tblOrgs"
)

// simInverse maps each link field to the table it points at and the field
// that mirrors it there.
var simInverse = map[string]struct{ table, linkedTable, inverse string }{
	"Program":  {simProjects, simPrograms, "Projects"},
	"Projects": {simPrograms, simProjects, "Program"},
	"Org":      {simPrograms, simOrgs, "Programs"},
	"Programs": {simOrgs, simPrograms, "Org"},
}

func newSimAirtable() *simAirtable {
	sim := &simAirtable{
		clock:    time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC),
		pageSize: 3,
		tables:   map[string]*simTable{},
		order:    []string{simProjects, simPrograms, simOrgs},
	}
	for _, id := range sim.order {
		sim.tables[id] = &simTable{id: id, name: map[string]string{simProjects: "Projects", simPrograms: "Programs", simOrgs: "Orgs"}[id], records: map[string]*simRecord{}}
	}
	return sim
}

func (s *simAirtable) now() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clock
}

func (s *simAirtable) advance(d time.Duration) {
	s.mu.Lock()
	s.clock = s.clock.Add(d)
	s.mu.Unlock()
}

// tick moves the clock one second per write so every edit has its own time.
func (s *simAirtable) tick() time.Time {
	s.clock = s.clock.Add(time.Second)
	return s.clock
}

func (s *simAirtable) create(tableID string, text map[string]string, number map[string]float64) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	id := "rec" + strconv.Itoa(1000000+s.nextID)
	record := &simRecord{id: id, created: s.tick(), text: map[string]string{}, number: map[string]float64{}, links: map[string][]string{}}
	for k, v := range text {
		record.text[k] = v
	}
	for k, v := range number {
		record.number[k] = v
	}
	if len(text)+len(number) > 0 {
		record.modified = record.created
	}
	s.tables[tableID].records[id] = record
	return id
}

func (s *simAirtable) setText(tableID, id, field, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.tables[tableID].records[id]
	record.text[field] = value
	record.modified = s.tick()
}

func (s *simAirtable) setNumber(tableID, id, field string, value float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.tables[tableID].records[id]
	record.number[field] = value
	record.modified = s.tick()
}

// setLinks replaces a link field and mirrors the change on the inverse side.
func (s *simAirtable) setLinks(tableID, id, field string, targets []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	wiring := simInverse[field]
	record := s.tables[tableID].records[id]
	at := s.tick()
	before := record.links[field]
	record.links[field] = append([]string(nil), targets...)
	record.modified = at
	other := s.tables[wiring.linkedTable]
	for _, target := range before {
		if !containsString(targets, target) {
			if linked := other.records[target]; linked != nil {
				linked.links[wiring.inverse] = removeString(linked.links[wiring.inverse], id)
				linked.modified = at
			}
		}
	}
	for _, target := range targets {
		if !containsString(before, target) {
			if linked := other.records[target]; linked != nil {
				linked.links[wiring.inverse] = append(linked.links[wiring.inverse], id)
				linked.modified = at
			}
		}
	}
}

// delete removes a record. It reports whether any other record linked to it,
// which is what makes the deletion visible to a delta.
func (s *simAirtable) delete(tableID, id string) (wasLinked bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.tables[tableID].records[id]
	if record == nil {
		return false
	}
	at := s.tick()
	for field, targets := range record.links {
		wiring := simInverse[field]
		for _, target := range targets {
			if linked := s.tables[wiring.linkedTable].records[target]; linked != nil {
				linked.links[wiring.inverse] = removeString(linked.links[wiring.inverse], id)
				linked.modified = at
				wasLinked = true
			}
		}
	}
	delete(s.tables[tableID].records, id)
	return wasLinked
}

func (s *simAirtable) ids(tableID string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.tables[tableID].records))
	for id := range s.tables[tableID].records {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (s *simAirtable) linksOf(tableID, id, field string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.tables[tableID].records[id].links[field]...)
}

func containsString(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

func removeString(values []string, value string) []string {
	out := values[:0:0]
	for _, v := range values {
		if v != value {
			out = append(out, v)
		}
	}
	return out
}

// fields renders a record the way Airtable's REST API does, computing every
// derived field from the current state. Callers hold s.mu.
func (s *simAirtable) fields(tableID string, record *simRecord) map[string]any {
	out := map[string]any{}
	for k, v := range record.text {
		if v != "" {
			out[k] = v
		}
	}
	for k, v := range record.number {
		out[k] = v
	}
	for k, v := range record.links {
		if len(v) > 0 {
			links := make([]any, 0, len(v))
			for _, id := range v {
				links = append(links, id)
			}
			out[k] = links
		}
	}
	switch tableID {
	case simProjects:
		var names []any
		label := record.text["Name"] + "/"
		for i, programID := range record.links["Program"] {
			if program := s.tables[simPrograms].records[programID]; program != nil {
				names = append(names, program.text["Name"])
				if i > 0 {
					label += ", "
				}
				label += program.text["Name"]
			}
		}
		if len(names) > 0 {
			out["Program Name"] = names
		}
		out["Label"] = label
	case simPrograms:
		out["Total Hours"] = s.programHours(record)
		out["Project Count"] = float64(len(record.links["Projects"]))
	case simOrgs:
		var total float64
		for _, programID := range record.links["Programs"] {
			if program := s.tables[simPrograms].records[programID]; program != nil {
				total += s.programHours(program)
			}
		}
		out["Grand Total"] = total
	}
	return out
}

func (s *simAirtable) programHours(program *simRecord) float64 {
	var total float64
	for _, projectID := range program.links["Projects"] {
		if project := s.tables[simProjects].records[projectID]; project != nil {
			total += project.number["Hours"]
		}
	}
	return total
}

func simOptions(value map[string]any) json.RawMessage {
	raw, _ := json.Marshal(value)
	return raw
}

func (s *simAirtable) ListBases(context.Context, string) ([]Base, error) {
	return []Base{{ID: "appSim", Name: "Simulated", PermissionLevel: "create"}}, nil
}

func (s *simAirtable) GetBaseSchema(context.Context, string, string) ([]Table, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schemaCalls++
	number := simOptions(map[string]any{"precision": 0})
	projects := Table{ID: simProjects, Name: "Projects", PrimaryFieldID: "fldPName", Fields: []Field{
		{ID: "fldPName", Name: "Name", Type: "singleLineText"},
		{ID: "fldPHours", Name: "Hours", Type: "number", Options: number},
		{ID: "fldPProgram", Name: "Program", Type: "multipleRecordLinks", Options: simOptions(map[string]any{"linkedTableId": simPrograms, "inverseLinkFieldId": "fldGProjects"})},
		{ID: "fldPProgramName", Name: "Program Name", Type: "multipleLookupValues", Options: simOptions(map[string]any{"recordLinkFieldId": "fldPProgram", "fieldIdInLinkedTable": "fldGName", "isValid": true})},
		{ID: "fldPLabel", Name: "Label", Type: "formula", Options: simOptions(map[string]any{"formula": "{fldPName}&'/'&{fldPProgram}", "referencedFieldIds": []string{"fldPName", "fldPProgram"}, "isValid": true})},
	}}
	if s.extraField {
		projects.Fields = append(projects.Fields, Field{ID: "fldPNotes", Name: "Notes", Type: "singleLineText"})
	}
	programs := Table{ID: simPrograms, Name: "Programs", PrimaryFieldID: "fldGName", Fields: []Field{
		{ID: "fldGName", Name: "Name", Type: "singleLineText"},
		{ID: "fldGProjects", Name: "Projects", Type: "multipleRecordLinks", Options: simOptions(map[string]any{"linkedTableId": simProjects, "inverseLinkFieldId": "fldPProgram"})},
		{ID: "fldGTotal", Name: "Total Hours", Type: "rollup", Options: simOptions(map[string]any{"recordLinkFieldId": "fldGProjects", "fieldIdInLinkedTable": "fldPHours", "isValid": true, "referencedFieldIds": []string{}})},
		{ID: "fldGCount", Name: "Project Count", Type: "count", Options: simOptions(map[string]any{"recordLinkFieldId": "fldGProjects", "isValid": true})},
		{ID: "fldGOrg", Name: "Org", Type: "multipleRecordLinks", Options: simOptions(map[string]any{"linkedTableId": simOrgs, "inverseLinkFieldId": "fldOPrograms"})},
	}}
	orgs := Table{ID: simOrgs, Name: "Orgs", PrimaryFieldID: "fldOName", Fields: []Field{
		{ID: "fldOName", Name: "Name", Type: "singleLineText"},
		{ID: "fldOPrograms", Name: "Programs", Type: "multipleRecordLinks", Options: simOptions(map[string]any{"linkedTableId": simPrograms, "inverseLinkFieldId": "fldGOrg"})},
		{ID: "fldOGrand", Name: "Grand Total", Type: "rollup", Options: simOptions(map[string]any{"recordLinkFieldId": "fldOPrograms", "fieldIdInLinkedTable": "fldGTotal", "isValid": true})},
	}}
	return []Table{projects, programs, orgs}, nil
}

func (s *simAirtable) ListRecordsPage(_ context.Context, _ string, _ string, tableID string, options ListRecordsPageOptions) (ListRecordsPageResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pageCalls++
	table := s.tables[tableID]
	if table == nil {
		return ListRecordsPageResult{}, fmt.Errorf("unknown table %s", tableID)
	}

	ids := make([]string, 0, len(table.records))
	for id, record := range table.records {
		if !options.ModifiedSince.IsZero() {
			effective := record.modified
			if effective.IsZero() {
				effective = record.created
			}
			if !effective.After(options.ModifiedSince) {
				continue
			}
		}
		if len(options.RecordIDs) > 0 && !containsString(options.RecordIDs, id) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)

	start := 0
	if options.Offset != "" {
		start, _ = strconv.Atoi(options.Offset)
	}
	end := min(start+s.pageSize, len(ids))
	result := ListRecordsPageResult{}
	for _, id := range ids[min(start, len(ids)):end] {
		record := table.records[id]
		result.Records = append(result.Records, Record{ID: id, CreatedTime: record.created, Fields: s.fields(tableID, record)})
	}
	if end < len(ids) {
		result.Offset = strconv.Itoa(end)
	}
	return result, nil
}

func (s *simAirtable) ListRecords(ctx context.Context, token, baseID, tableID string) ([]Record, error) {
	var records []Record
	options := ListRecordsPageOptions{}
	for {
		page, err := s.ListRecordsPage(ctx, token, baseID, tableID, options)
		if err != nil {
			return nil, err
		}
		records = append(records, page.Records...)
		if page.Offset == "" {
			return records, nil
		}
		options.Offset = page.Offset
	}
}

func (s *simAirtable) counts() (schema, pages int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.schemaCalls, s.pageCalls
}
