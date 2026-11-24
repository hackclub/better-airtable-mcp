//go:build integration

package mcp_test

import "net/http"

// isAirtableRecordList reports whether r lists records of the table at
// tablePath, in either form the Airtable client uses: a plain GET, or the
// POST listRecords variant that carries a filter in the body. The fakes in
// these tests answer both with every record; a filtered listing that returns
// extra, unchanged records only rewrites rows with the same values.
func isAirtableRecordList(r *http.Request, tablePath string) bool {
	if r.URL.Path == tablePath && r.Method == http.MethodGet {
		return true
	}
	return r.URL.Path == tablePath+"/listRecords" && r.Method == http.MethodPost
}
