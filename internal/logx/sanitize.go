package logx

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

const (
	MaxErrorPreviewLength = 160
	MaxSQLPreviewLength   = 200
)

var (
	approvalURLPattern  = regexp.MustCompile(`https?://[^\s"']+/approve/op_[A-Za-z0-9_-]+|/approve/op_[A-Za-z0-9_-]+`)
	approvalIDPattern   = regexp.MustCompile(`\bop_[A-Za-z0-9_-]+\b`)
	bearerPattern       = regexp.MustCompile(`(?i)Bearer\s+[A-Za-z0-9._~+/-]+=*`)
	authorizationHeader = regexp.MustCompile(`(?i)(Authorization["=: ]+)(Bearer\s+)?[^"\s,}]+`)
	jsonSecretPattern   = regexp.MustCompile(`(?i)"(access_token|refresh_token|client_secret|code_verifier|code|state)"\s*:\s*"[^"]*"`)
	formSecretPattern   = regexp.MustCompile(`(?i)(access_token|refresh_token|client_secret|code_verifier|code|state)=([^&\s]+)`)
	uuidLikePattern     = regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	longOpaquePattern   = regexp.MustCompile(`\b[A-Za-z0-9_-]{24,}\b`)
)

func HashString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func ApprovalOperationIDHash(operationID string) string {
	return HashString(strings.TrimSpace(operationID))
}

func RedactString(value string) string {
	if strings.TrimSpace(value) == "" {
		return value
	}

	redacted := approvalURLPattern.ReplaceAllString(value, "[REDACTED_APPROVAL_URL]")
	redacted = approvalIDPattern.ReplaceAllString(redacted, "[REDACTED_APPROVAL_OPERATION_ID]")
	redacted = bearerPattern.ReplaceAllString(redacted, "Bearer [REDACTED]")
	redacted = authorizationHeader.ReplaceAllString(redacted, `${1}[REDACTED]`)
	redacted = jsonSecretPattern.ReplaceAllStringFunc(redacted, func(segment string) string {
		parts := strings.SplitN(segment, ":", 2)
		if len(parts) != 2 {
			return `"[REDACTED]":"[REDACTED]"`
		}
		return parts[0] + `:"[REDACTED]"`
	})
	redacted = formSecretPattern.ReplaceAllString(redacted, `${1}=[REDACTED]`)
	return redacted
}

func ErrorPreview(err error) string {
	if err == nil {
		return ""
	}
	return Truncate(RedactString(err.Error()), MaxErrorPreviewLength)
}

func ErrorKind(err error) string {
	if err == nil {
		return ""
	}

	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "context canceled"):
		return "canceled"
	case strings.Contains(text, "deadline exceeded"), strings.Contains(text, "timeout"):
		return "timeout"
	case strings.Contains(text, "rate limit"), strings.Contains(text, "too many requests"):
		return "rate_limit"
	case strings.Contains(text, "invalid or expired bearer token"), strings.Contains(text, "missing bearer token"), strings.Contains(text, "invalid_grant"), strings.Contains(text, "authorization"):
		return "auth"
	case strings.Contains(text, "decrypt"), strings.Contains(text, "encrypt"):
		return "crypto"
	case strings.Contains(text, "invalid "), strings.Contains(text, "must "), strings.Contains(text, "required"), strings.Contains(text, "unsupported"), strings.Contains(text, "disallowed"):
		return "validation"
	case strings.Contains(text, "not found"), strings.Contains(text, "unknown "):
		return "not_found"
	case strings.Contains(text, "airtable"), strings.Contains(text, "oauth"):
		return "external_api"
	case strings.Contains(text, "postgres"), strings.Contains(text, "pgx"), strings.Contains(text, "sqlstate"):
		return "db"
	default:
		return "internal"
	}
}

func Truncate(value string, max int) string {
	if max <= 0 || len(value) <= max {
		return value
	}
	if max <= 3 {
		return value[:max]
	}
	return value[:max-3] + "..."
}

func SanitizeExternalBody(value string) string {
	return Truncate(RedactString(strings.TrimSpace(value)), MaxErrorPreviewLength)
}

func SanitizeRedirectURI(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return Truncate(RedactString(raw), MaxErrorPreviewLength)
	}
	return parsed.Scheme + "://" + parsed.Host + parsed.Path
}

func SanitizeSQLPreview(sql string) string {
	var builder strings.Builder
	builder.Grow(len(sql))

	for index := 0; index < len(sql); {
		char := sql[index]
		switch {
		case char == '\'':
			builder.WriteString("?")
			index = skipQuoted(sql, index+1, '\'')
		case char == '"':
			builder.WriteString("?")
			index = skipQuoted(sql, index+1, '"')
		case char == '`':
			builder.WriteString("?")
			index = skipQuoted(sql, index+1, '`')
		case char == '-' && index+1 < len(sql) && sql[index+1] == '-':
			for index < len(sql) && sql[index] != '\n' {
				index++
			}
		case char == '/' && index+1 < len(sql) && sql[index+1] == '*':
			index += 2
			for index+1 < len(sql) && !(sql[index] == '*' && sql[index+1] == '/') {
				index++
			}
			if index+1 < len(sql) {
				index += 2
			}
		case isNumericLiteralStart(sql, index):
			builder.WriteString("?")
			index = skipNumericLiteral(sql, index)
		default:
			builder.WriteByte(char)
			index++
		}
	}

	preview := strings.Join(strings.Fields(builder.String()), " ")
	preview = uuidLikePattern.ReplaceAllString(preview, "?")
	preview = longOpaquePattern.ReplaceAllStringFunc(preview, func(candidate string) string {
		if strings.Contains(candidate, ".") {
			return candidate
		}
		return "?"
	})
	return Truncate(preview, MaxSQLPreviewLength)
}

func SQLPreviewAndHash(sql string) map[string]any {
	return map[string]any{
		"sql_preview": SanitizeSQLPreview(sql),
		"sql_sha256":  HashString(strings.TrimSpace(sql)),
	}
}

func SummarizeToolArguments(toolName string, raw json.RawMessage) map[string]any {
	summary := map[string]any{
		"payload_bytes": len(strings.TrimSpace(string(raw))),
	}

	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return summary
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
		summary["invalid_json"] = true
		return summary
	}

	keys := make([]string, 0, len(payload))
	for key := range payload {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	summary["top_level_keys"] = keys

	switch toolName {
	case "list_bases":
		if query, ok := payload["query"].(string); ok && strings.TrimSpace(query) != "" {
			summary["query_hash"] = HashString(strings.TrimSpace(query))
			summary["query_length"] = len(strings.TrimSpace(query))
		}
	case "list_schema", "sync":
		if base, ok := payload["base"].(string); ok {
			summary["base_ref"] = summarizeReference(base, "app")
		}
	case "query":
		if base, ok := payload["base"].(string); ok {
			summary["base_ref"] = summarizeReference(base, "app")
		}
		if queries, ok := payload["sql"].([]any); ok {
			previewItems := make([]map[string]any, 0, len(queries))
			for _, item := range queries {
				sql, ok := item.(string)
				if !ok {
					continue
				}
				previewItems = append(previewItems, SQLPreviewAndHash(sql))
			}
			summary["query_count"] = len(previewItems)
			summary["queries"] = previewItems
		}
	case "mutate":
		if base, ok := payload["base"].(string); ok {
			summary["base_ref"] = summarizeReference(base, "app")
		}
		if operations, ok := payload["operations"].([]any); ok {
			operationSummaries := make([]map[string]any, 0, len(operations))
			totalRecords := 0
			for _, item := range operations {
				operation, ok := item.(map[string]any)
				if !ok {
					continue
				}
				recordCount := 0
				fieldNames := map[string]struct{}{}
				if records, ok := operation["records"].([]any); ok {
					recordCount = len(records)
					for _, recordItem := range records {
						record, ok := recordItem.(map[string]any)
						if !ok {
							continue
						}
						fields, ok := record["fields"].(map[string]any)
						if !ok {
							continue
						}
						for fieldName := range fields {
							fieldNames[fieldName] = struct{}{}
						}
					}
				}
				totalRecords += recordCount
				entry := map[string]any{
					"type":             strings.TrimSpace(stringValue(operation["type"])),
					"record_count":     recordCount,
					"field_name_count": len(fieldNames),
				}
				if table := stringValue(operation["table"]); table != "" {
					entry["table_ref"] = summarizeReference(table, "tbl")
				}
				operationSummaries = append(operationSummaries, entry)
			}
			summary["operation_count"] = len(operationSummaries)
			summary["record_count"] = totalRecords
			summary["operations"] = operationSummaries
		}
	case "check_operation":
		if operationID := stringValue(payload["operation_id"]); operationID != "" {
			if strings.HasPrefix(operationID, "sync_") {
				summary["sync_operation_id"] = operationID
			} else {
				summary["approval_operation_id_hash"] = ApprovalOperationIDHash(operationID)
			}
		}
	}

	return summary
}

func summarizeReference(value, safePrefix string) map[string]any {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return map[string]any{"kind": "empty"}
	}
	if strings.HasPrefix(trimmed, safePrefix) {
		return map[string]any{"kind": "id", "id": trimmed}
	}
	return map[string]any{
		"kind":   "hash",
		"sha256": HashString(trimmed),
		"length": len(trimmed),
	}
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return ""
	}
}

func skipQuoted(input string, start int, quote byte) int {
	for index := start; index < len(input); index++ {
		if input[index] != quote {
			continue
		}
		if index+1 < len(input) && input[index+1] == quote {
			index++
			continue
		}
		return index + 1
	}
	return len(input)
}

func isNumericLiteralStart(input string, index int) bool {
	if index >= len(input) {
		return false
	}
	if !unicode.IsDigit(rune(input[index])) {
		return false
	}
	if index > 0 {
		prior := rune(input[index-1])
		if unicode.IsLetter(prior) || prior == '_' {
			return false
		}
	}
	return true
}

func skipNumericLiteral(input string, start int) int {
	index := start
	for index < len(input) {
		char := input[index]
		if unicode.IsDigit(rune(char)) || char == '.' {
			index++
			continue
		}
		break
	}
	return index
}

func AttrsFromMap(prefix string, value map[string]any) []any {
	if len(value) == 0 {
		return nil
	}

	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	attrs := make([]any, 0, len(keys)*2)
	for _, key := range keys {
		attrs = append(attrs, prefix+key, normalizeAttrValue(value[key]))
	}
	return attrs
}

func normalizeAttrValue(value any) any {
	switch typed := value.(type) {
	case []map[string]any:
		return typed
	case map[string]any:
		return typed
	case []string:
		return typed
	case []any:
		return typed
	case int:
		return typed
	case int64:
		return typed
	case bool:
		return typed
	case string:
		return typed
	default:
		return fmt.Sprint(value)
	}
}

func IntString(value int) string {
	return strconv.Itoa(value)
}
