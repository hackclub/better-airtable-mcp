package duckdb

import (
	"fmt"
	"strings"
	"unicode"
)

type SanitizedName struct {
	Original  string
	Sanitized string
}

type TypeMapping struct {
	DuckDBType string
	Omitted    bool
}

var airtableTypeMappings = map[string]TypeMapping{
	"singleLineText":      {DuckDBType: "VARCHAR"},
	"multilineText":       {DuckDBType: "VARCHAR"},
	"richText":            {DuckDBType: "VARCHAR"},
	"email":               {DuckDBType: "VARCHAR"},
	"url":                 {DuckDBType: "VARCHAR"},
	"phoneNumber":         {DuckDBType: "VARCHAR"},
	"number":              {DuckDBType: "DOUBLE"},
	"percent":             {DuckDBType: "DOUBLE"},
	"currency":            {DuckDBType: "DOUBLE"},
	"autoNumber":          {DuckDBType: "BIGINT"},
	"checkbox":            {DuckDBType: "BOOLEAN"},
	"date":                {DuckDBType: "TIMESTAMP"},
	"dateTime":            {DuckDBType: "TIMESTAMP"},
	"createdTime":         {DuckDBType: "TIMESTAMP"},
	"lastModifiedTime":    {DuckDBType: "TIMESTAMP"},
	"singleSelect":        {DuckDBType: "VARCHAR"},
	"multipleSelects":     {DuckDBType: "VARCHAR[]"},
	"multipleRecordLinks": {DuckDBType: "VARCHAR[]"},
	"lookup":              {DuckDBType: "JSON"},
	"rollup":              {DuckDBType: "VARCHAR"},
	"formula":             {DuckDBType: "VARCHAR"},
	"multipleAttachments": {DuckDBType: "JSON"},
	"createdBy":           {DuckDBType: "VARCHAR"},
	"lastModifiedBy":      {DuckDBType: "VARCHAR"},
	"barcode":             {DuckDBType: "VARCHAR"},
	"button":              {Omitted: true},
	"rating":              {DuckDBType: "BIGINT"},
	"duration":            {DuckDBType: "DOUBLE"},
}

func SanitizeIdentifier(name string) string {
	var builder strings.Builder
	lastUnderscore := false

	for _, r := range name {
		switch {
		case r <= unicode.MaxASCII && (unicode.IsLetter(r) || unicode.IsDigit(r)):
			builder.WriteRune(unicode.ToLower(r))
			lastUnderscore = false
		case r == '_' || r == '-' || unicode.IsSpace(r) || isASCIISeparator(r):
			if !lastUnderscore {
				builder.WriteByte('_')
				lastUnderscore = true
			}
		case r > unicode.MaxASCII:
			if unicode.IsSpace(r) && !lastUnderscore {
				builder.WriteByte('_')
				lastUnderscore = true
			}
		default:
			if !lastUnderscore {
				builder.WriteByte('_')
				lastUnderscore = true
			}
		}
	}

	sanitized := strings.Trim(builder.String(), "_")
	if sanitized == "" {
		return "t_"
	}
	if sanitized[0] >= '0' && sanitized[0] <= '9' {
		return "t_" + sanitized
	}

	return sanitized
}

func SanitizeIdentifiers(names []string) []SanitizedName {
	return sanitizeIdentifiers(names, nil)
}

func SanitizeFieldIdentifiers(names []string) []SanitizedName {
	return sanitizeIdentifiers(names, map[string]string{
		"id":           "_airtable_id",
		"created_time": "_airtable_created_time",
	})
}

func sanitizeIdentifiers(names []string, reserved map[string]string) []SanitizedName {
	results := make([]SanitizedName, 0, len(names))
	used := make(map[string]struct{}, len(names))

	for _, name := range names {
		base := SanitizeIdentifier(name)
		if replacement, ok := reserved[base]; ok {
			base = replacement
		}

		// Bump the suffix until the candidate is free of every name already
		// emitted — including previously *generated* suffixed names, so an
		// input whose base equals an earlier "<name>_2" cannot collide.
		candidate := base
		for i := 2; ; i++ {
			if _, taken := used[candidate]; !taken {
				break
			}
			candidate = fmt.Sprintf("%s_%d", strings.TrimRight(base, "_"), i)
		}

		used[candidate] = struct{}{}
		results = append(results, SanitizedName{
			Original:  name,
			Sanitized: candidate,
		})
	}

	return results
}

func AirtableTypeToDuckDBType(airtableType string) (TypeMapping, bool) {
	mapping, ok := airtableTypeMappings[airtableType]
	return mapping, ok
}

func isASCIISeparator(r rune) bool {
	switch r {
	case '/', '\\', '.', ',', ':', ';', '!', '?', '&', '+', '=', '(', ')', '[', ']', '{', '}', '#', '@', '%':
		return true
	default:
		return false
	}
}
