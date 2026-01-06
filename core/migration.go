package core

import (
	"os"
	"strings"
)

type Migration struct {
	Statements []string
	Rollback   []string
	Breaking   []string
	Notes      []string
	Unresolved []string
}

func (m *Migration) String() string {
	var sb strings.Builder
	sb.WriteString("-- schemift migration\n")
	sb.WriteString("-- Review before running in production.\n")

	writeCommentSection := func(title string, items []string) {
		if len(items) == 0 {
			return
		}
		sb.WriteString("\n-- " + title + "\n")
		for _, item := range items {
			for _, line := range splitCommentLines(item) {
				if line == "" {
					continue
				}
				sb.WriteString("-- - " + line + "\n")
			}
		}
	}

	writeCommentSection("BREAKING CHANGES (manual review required)", m.Breaking)
	writeCommentSection("UNRESOLVED (cannot auto-generate safely)", m.Unresolved)
	writeCommentSection("NOTES", m.Notes)

	if len(m.Statements) == 0 {
		sb.WriteString("\n-- No SQL statements generated.\n")
		if len(m.Rollback) > 0 {
			sb.WriteString("\n-- ROLLBACK SQL (run separately)\n")
			writeRollbackAsComments(&sb, m.Rollback)
		}
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

	if len(m.Rollback) > 0 {
		sb.WriteString("\n-- ROLLBACK SQL (run separately)\n")
		writeRollbackAsComments(&sb, m.Rollback)
	}

	return sb.String()
}

func (m *Migration) RollbackString() string {
	var sb strings.Builder
	sb.WriteString("-- schemift rollback\n")
	sb.WriteString("-- Run to revert the migration (review carefully).\n")

	if len(m.Rollback) == 0 {
		sb.WriteString("\n-- No rollback statements generated.\n")
		return sb.String()
	}

	sb.WriteString("\n-- SQL\n")
	for i := len(m.Rollback) - 1; i >= 0; i-- {
		stmt := strings.TrimSpace(m.Rollback[i])
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

func splitCommentLines(s string) []string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(line)
	}
	return lines
}

func (m *Migration) SaveToFile(path string) error {
	return os.WriteFile(path, []byte(m.String()), 0644)
}

func (m *Migration) SaveRollbackToFile(path string) error {
	return os.WriteFile(path, []byte(m.RollbackString()), 0644)
}

func (m *Migration) AddStatement(stmt string) {
	if stmt = strings.TrimSpace(stmt); stmt != "" {
		m.Statements = append(m.Statements, stmt)
	}
}

func (m *Migration) AddRollbackStatement(stmt string) {
	if stmt = strings.TrimSpace(stmt); stmt != "" {
		m.Rollback = append(m.Rollback, stmt)
	}
}

func (m *Migration) AddStatementWithRollback(up, down string) {
	m.AddStatement(up)
	m.AddRollbackStatement(down)
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
	m.Rollback = dedupeStrings(m.Rollback)
}

func writeRollbackAsComments(sb *strings.Builder, rollback []string) {
	for i := len(rollback) - 1; i >= 0; i-- {
		for _, line := range splitCommentLines(rollback[i]) {
			if line == "" {
				continue
			}
			sb.WriteString("-- ")
			sb.WriteString(line)
			if !strings.HasSuffix(line, ";") {
				sb.WriteString(";")
			}
			sb.WriteString("\n")
		}
	}
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
