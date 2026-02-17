package core

import (
	"fmt"
)

// validateIndexes checks for duplicate index names and verifies that every
// index column references an existing table column.
func (t *Table) validateIndexes() error {
	if err := t.validateIndexNames(); err != nil {
		return err
	}
	return t.validateIndexColumns()
}

func (t *Table) validateIndexNames() error {
	seen := make(map[string]bool, len(t.Indexes))
	for _, idx := range t.Indexes {
		if idx.Name == "" {
			continue
		}
		if err := validateName(idx.Name, nil, nil, false); err != nil {
			return fmt.Errorf("index %w", err)
		}
		if seen[idx.Name] {
			return fmt.Errorf("duplicate index name %q", idx.Name)
		}
		seen[idx.Name] = true
	}
	return nil
}

func (t *Table) validateIndexColumns() error {
	for _, idx := range t.Indexes {
		if len(idx.Columns) == 0 {
			name := idx.Name
			if name == "" {
				name = "(unnamed)"
			}
			return fmt.Errorf("index %s has no columns", name)
		}
		for _, ic := range idx.Columns {
			if t.FindColumn(ic.Name) == nil {
				return fmt.Errorf("index %q references nonexistent column %q", idx.Name, ic.Name)
			}
		}
	}
	return nil
}
