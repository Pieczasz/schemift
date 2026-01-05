package core

import (
	"os"
	"strings"
)

type Migration struct {
	Statements []string
	Breaking   []string
	Notes      []string
	Unresolved []string
}

func (m *Migration) String() string {
	var sb strings.Builder
	sb.WriteString("-- schemift migration\n")
	sb.WriteString("-- Review before running in production.\n")

	if len(m.Breaking) > 0 {
		sb.WriteString("\n-- BREAKING CHANGES (manual review required)\n")
		for _, b := range m.Breaking {
			sb.WriteString("-- - " + b + "\n")
		}
	}

	if len(m.Unresolved) > 0 {
		sb.WriteString("\n-- UNRESOLVED (cannot auto-generate safely)\n")
		for _, u := range m.Unresolved {
			sb.WriteString("-- - " + u + "\n")
		}
	}

	if len(m.Notes) > 0 {
		sb.WriteString("\n-- NOTES\n")
		for _, n := range m.Notes {
			sb.WriteString("-- - " + n + "\n")
		}
	}

	if len(m.Statements) == 0 {
		sb.WriteString("\n-- No SQL statements generated.\n")
		return sb.String()
	}

	sb.WriteString("\n-- SQL\n")
	for _, stmt := range m.Statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		sb.WriteString(stmt)
		if !strings.HasSuffix(stmt, ";") {
			sb.WriteString(";")
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

func (m *Migration) SaveToFile(path string) error {
	return os.WriteFile(path, []byte(m.String()), 0644)
}

func (m *Migration) AddStatement(stmt string) {
	if stmt = strings.TrimSpace(stmt); stmt != "" {
		m.Statements = append(m.Statements, stmt)
	}
}

func (m *Migration) AddBreaking(msg string) {
	if msg = strings.TrimSpace(msg); msg != "" {
		m.Breaking = append(m.Breaking, msg)
	}
}

func (m *Migration) AddNote(msg string) {
	if msg = strings.TrimSpace(msg); msg != "" {
		m.Notes = append(m.Notes, msg)
	}
}

func (m *Migration) AddUnresolved(msg string) {
	if msg = strings.TrimSpace(msg); msg != "" {
		m.Unresolved = append(m.Unresolved, msg)
	}
}

func (m *Migration) Dedupe() {
	m.Breaking = dedupeStrings(m.Breaking)
	m.Notes = dedupeStrings(m.Notes)
	m.Unresolved = dedupeStrings(m.Unresolved)
}

func dedupeStrings(items []string) []string {
	seen := make(map[string]struct{}, len(items))
	var out []string
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}
