package sanitize

import "strings"

func SanitizeIdentifier(name string) string {
	name = strings.TrimSpace(name)

	var result strings.Builder
	for i, r := range name {
		if i == 0 {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_' {
				result.WriteRune(r)
			} else {
				result.WriteRune('_')
			}
		} else {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '$' {
				result.WriteRune(r)
			} else {
				result.WriteRune('_')
			}
		}
	}
	return result.String()
}
