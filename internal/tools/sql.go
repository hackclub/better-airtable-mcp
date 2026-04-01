package tools

import (
	"fmt"
	"strings"
	"unicode"
)

type NormalizedQuery struct {
	SQL                string
	ExecutionSQL       string
	EffectiveLimit     int
	ServerAppliedLimit bool
}

type sqlTokenKind int

const (
	sqlTokenWord sqlTokenKind = iota
	sqlTokenSemicolon
)

type sqlToken struct {
	Kind  sqlTokenKind
	Value string
}

var blockedSQLKeywords = map[string]struct{}{
	"ALTER":    {},
	"ATTACH":   {},
	"CALL":     {},
	"COPY":     {},
	"CREATE":   {},
	"DELETE":   {},
	"DETACH":   {},
	"DROP":     {},
	"EXPORT":   {},
	"IMPORT":   {},
	"INSERT":   {},
	"INSTALL":  {},
	"LOAD":     {},
	"MERGE":    {},
	"PRAGMA":   {},
	"REPLACE":  {},
	"SET":      {},
	"TRUNCATE": {},
	"UPDATE":   {},
	"USE":      {},
	"VACUUM":   {},
}

func NormalizeQuery(sql string, requestedLimit, defaultLimit, maxLimit int) (NormalizedQuery, error) {
	if err := ValidateReadOnlySQL(sql); err != nil {
		return NormalizedQuery{}, err
	}

	effectiveLimit, err := ResolveQueryLimit(requestedLimit, defaultLimit, maxLimit)
	if err != nil {
		return NormalizedQuery{}, err
	}

	normalizedSQL := strings.TrimSpace(sql)
	executionSQL := normalizedSQL
	serverAppliedLimit := false
	if !HasTopLevelLimit(normalizedSQL) {
		normalizedSQL = ApplyTopLevelLimit(normalizedSQL, effectiveLimit)
		executionSQL = ApplyTopLevelLimit(executionSQL, effectiveLimit+1)
		serverAppliedLimit = true
	}

	return NormalizedQuery{
		SQL:                normalizedSQL,
		ExecutionSQL:       executionSQL,
		EffectiveLimit:     effectiveLimit,
		ServerAppliedLimit: serverAppliedLimit,
	}, nil
}

func ResolveQueryLimit(requestedLimit, defaultLimit, maxLimit int) (int, error) {
	switch {
	case defaultLimit <= 0:
		return 0, fmt.Errorf("default limit must be greater than zero")
	case maxLimit <= 0:
		return 0, fmt.Errorf("max limit must be greater than zero")
	case defaultLimit > maxLimit:
		return 0, fmt.Errorf("default limit must be less than or equal to max limit")
	case requestedLimit < 0:
		return 0, fmt.Errorf("limit must be greater than or equal to zero")
	case requestedLimit == 0:
		return defaultLimit, nil
	case requestedLimit > maxLimit:
		return 0, fmt.Errorf("limit must be less than or equal to %d", maxLimit)
	default:
		return requestedLimit, nil
	}
}

func ValidateReadOnlySQL(sql string) error {
	tokens, err := scanSQLTokens(sql)
	if err != nil {
		return err
	}
	if len(tokens) == 0 {
		return fmt.Errorf("sql is required")
	}

	firstWord := ""
	seenSemicolon := false

	for _, token := range tokens {
		switch token.Kind {
		case sqlTokenSemicolon:
			if seenSemicolon {
				return fmt.Errorf("sql must contain exactly one statement")
			}
			seenSemicolon = true
		case sqlTokenWord:
			if seenSemicolon {
				return fmt.Errorf("sql must contain exactly one statement")
			}
			if firstWord == "" {
				firstWord = token.Value
			}
			if _, blocked := blockedSQLKeywords[token.Value]; blocked {
				return fmt.Errorf("sql contains a disallowed keyword: %s", token.Value)
			}
		}
	}

	if firstWord == "" {
		return fmt.Errorf("sql must contain a query")
	}
	if firstWord != "SELECT" && firstWord != "WITH" {
		return fmt.Errorf("sql must start with SELECT or WITH")
	}

	return nil
}

func HasTopLevelLimit(sql string) bool {
	tokens, err := scanSQLTokens(sql)
	if err != nil {
		return false
	}

	for _, token := range tokens {
		if token.Kind == sqlTokenWord && token.Value == "LIMIT" {
			return true
		}
	}

	return false
}

func ApplyTopLevelLimit(sql string, limit int) string {
	trimmed := strings.TrimSpace(sql)
	hadSemicolon := strings.HasSuffix(trimmed, ";")
	trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))

	if hadSemicolon {
		return fmt.Sprintf("%s LIMIT %d;", trimmed, limit)
	}

	return fmt.Sprintf("%s LIMIT %d", trimmed, limit)
}

func scanSQLTokens(sql string) ([]sqlToken, error) {
	input := strings.TrimSpace(sql)
	if input == "" {
		return nil, nil
	}

	tokens := make([]sqlToken, 0, 16)

	for index := 0; index < len(input); {
		switch {
		case startsWith(input, index, "--"):
			index = skipLineComment(input, index+2)
		case startsWith(input, index, "/*"):
			next, err := skipBlockComment(input, index+2)
			if err != nil {
				return nil, err
			}
			index = next
		case input[index] == '\'':
			next, err := skipQuotedString(input, index+1, '\'')
			if err != nil {
				return nil, err
			}
			index = next
		case input[index] == '"':
			next, err := skipQuotedString(input, index+1, '"')
			if err != nil {
				return nil, err
			}
			index = next
		case input[index] == '`':
			next, err := skipBacktickIdentifier(input, index+1)
			if err != nil {
				return nil, err
			}
			index = next
		case input[index] == ';':
			tokens = append(tokens, sqlToken{Kind: sqlTokenSemicolon})
			index++
		case isWordStart(rune(input[index])):
			start := index
			index++
			for index < len(input) && isWordPart(rune(input[index])) {
				index++
			}
			tokens = append(tokens, sqlToken{
				Kind:  sqlTokenWord,
				Value: strings.ToUpper(input[start:index]),
			})
		default:
			index++
		}
	}

	return tokens, nil
}

func startsWith(input string, index int, prefix string) bool {
	return len(input[index:]) >= len(prefix) && input[index:index+len(prefix)] == prefix
}

func skipLineComment(input string, index int) int {
	for index < len(input) && input[index] != '\n' {
		index++
	}
	return index
}

func skipBlockComment(input string, index int) (int, error) {
	for index < len(input)-1 {
		if input[index] == '*' && input[index+1] == '/' {
			return index + 2, nil
		}
		index++
	}

	return 0, fmt.Errorf("unterminated block comment")
}

func skipQuotedString(input string, index int, delimiter byte) (int, error) {
	for index < len(input) {
		if input[index] == delimiter {
			if index+1 < len(input) && input[index+1] == delimiter {
				index += 2
				continue
			}
			return index + 1, nil
		}
		index++
	}

	return 0, fmt.Errorf("unterminated quoted string")
}

func skipBacktickIdentifier(input string, index int) (int, error) {
	for index < len(input) {
		if input[index] == '`' {
			return index + 1, nil
		}
		index++
	}

	return 0, fmt.Errorf("unterminated quoted identifier")
}

func isWordStart(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

func isWordPart(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
