package core

import (
	"errors"
	"fmt"
	"regexp"
)

func (db *Database) validateTableUniqueness() error {
	seenTables := make(map[string]bool, len(db.Tables))
	for _, table := range db.Tables {
		if seenTables[table.Name] {
			return fmt.Errorf("duplicate table name %q", table.Name)
		}
		seenTables[table.Name] = true
	}
	return nil
}

func (db *Database) validateAndSynthesizeConstraints() error {
	for _, table := range db.Tables {
		if err := table.validatePrimaryKeyConflict(); err != nil {
			return fmt.Errorf("table %q: %w", table.Name, err)
		}
		table.synthesizeConstraints()
	}
	return nil
}

func (db *Database) validateTableStructures(nameRe *regexp.Regexp) error {
	for _, table := range db.Tables {
		if err := table.Validate(db.Validation, nameRe); err != nil {
			return err
		}
	}
	return nil
}

// Validate checks a single table for structural correctness.
func (t *Table) Validate(rules *ValidationRules, nameRe *regexp.Regexp) error {
	if err := t.validateNameAndOptions(rules, nameRe); err != nil {
		return err
	}
	if err := t.validateColumns(rules, nameRe); err != nil {
		return err
	}
	if err := t.validateConstraints(); err != nil {
		return err
	}
	if err := t.validateTimestamps(); err != nil {
		return err
	}
	return t.validateIndexes()
}

func (t *Table) validateNameAndOptions(rules *ValidationRules, nameRe *regexp.Regexp) error {
	if err := validateName(t.Name, rules, nameRe, true); err != nil {
		return fmt.Errorf("table %w", err)
	}
	if err := t.Options.Validate(); err != nil {
		return fmt.Errorf("table %q: %w", t.Name, err)
	}
	return nil
}

func (t *Table) validateColumns(rules *ValidationRules, nameRe *regexp.Regexp) error {
	if len(t.Columns) == 0 {
		return fmt.Errorf("table %q has no columns", t.Name)
	}
	seenCols := make(map[string]bool, len(t.Columns))
	for _, col := range t.Columns {
		if seenCols[col.Name] {
			return fmt.Errorf("table %q: duplicate column name %q", t.Name, col.Name)
		}
		seenCols[col.Name] = true
	}
	for _, col := range t.Columns {
		if err := col.Validate(rules, nameRe); err != nil {
			return fmt.Errorf("table %q: %w", t.Name, err)
		}
	}
	return nil
}

func (opt *TableOptions) Validate() error {
	// Dialect-specific validations can be added here as needed.
	return nil
}

// validatePKConflict ensures a table doesn't define primary keys both at the
// column level (primary_key = true) and in the constraints section.
func (t *Table) validatePrimaryKeyConflict() error {
	hasColumnPK := false
	for _, col := range t.Columns {
		if col.PrimaryKey {
			hasColumnPK = true
			break
		}
	}
	constraintPKCount := 0
	for _, con := range t.Constraints {
		if con.Type == ConstraintPrimaryKey {
			constraintPKCount++
		}
	}
	if constraintPKCount > 1 {
		return errors.New("multiple PRIMARY KEY constraints declared; a table can have at most one primary key")
	}
	if hasColumnPK && constraintPKCount > 0 {
		return errors.New("primary key declared on both column(s) and in constraints section")
	}
	return nil
}

// validateTimestamps checks that the created and updated timestamp columns
// resolve to distinct names and follow naming rules.
func (t *Table) validateTimestamps() error {
	if t.Timestamps == nil || !t.Timestamps.Enabled {
		return nil
	}
	createdCol := "created_at"
	updatedCol := "updated_at"
	if t.Timestamps.CreatedColumn != "" {
		if err := validateName(t.Timestamps.CreatedColumn, nil, nil, false); err != nil {
			return fmt.Errorf("timestamp created_column %w", err)
		}
		createdCol = t.Timestamps.CreatedColumn
	}
	if t.Timestamps.UpdatedColumn != "" {
		if err := validateName(t.Timestamps.UpdatedColumn, nil, nil, false); err != nil {
			return fmt.Errorf("timestamp updated_column %w", err)
		}
		updatedCol = t.Timestamps.UpdatedColumn
	}
	if createdCol == updatedCol {
		return fmt.Errorf("timestamps created_column and updated_column resolve to the same name %q", createdCol)
	}
	return nil
}
