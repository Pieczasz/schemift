// Package sanitize contains logic for sanitizing various user inputs e.g. table/column names.
package sanitize

import "strings"

func Identifier(name string) string {
	name = strings.TrimSpace(name)

	var result strings.Builder
	for i, r := range name {
		if IdentifierChar(r, i == 0) {
			result.WriteRune(r)
		} else {
			result.WriteRune('_')
		}
	}
	return result.String()
}

func IdentifierChar(r rune, isFirst bool) bool {
	isLetter := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
	isUnderscore := r == '_'
	if isFirst {
		return isLetter || isUnderscore
	}
	isDigit := (r >= '0' && r <= '9')
	return isLetter || isDigit || isUnderscore || r == '$'
}
