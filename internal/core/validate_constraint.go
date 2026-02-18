package core

import (
	"fmt"
)

// validateConstraints checks for duplicate constraint names, missing columns,
// and incomplete FK definitions.
func (t *Table) validateConstraints() error {
	seen := make(map[string]bool, len(t.Constraints))
	for _, con := range t.Constraints {
		if con.Name == "" {
			continue
		}
		if err := validateName(con.Name, nil, nil, false); err != nil {
			return fmt.Errorf("constraint %q: %w", con.Name, err)
		}
		if seen[con.Name] {
			return fmt.Errorf("duplicate constraint name %q", con.Name)
		}
		seen[con.Name] = true
	}

	for _, con := range t.Constraints {
		if err := t.validateConstraintColumns(con); err != nil {
			return err
		}
	}

	return nil
}

// validateConstraintColumns verifies a single constraint's columns exist, are
// non-empty (except CHECK), and that FK constraints have referenced_table and
// referenced_columns.
func (t *Table) validateConstraintColumns(con *Constraint) error {
	if con.Type == ConstraintCheck {
		return nil
	}
	if len(con.Columns) == 0 {
		return fmt.Errorf("constraint %q (%s) has no columns", con.Name, con.Type)
	}
	for _, colName := range con.Columns {
		if t.FindColumn(colName) == nil {
			return fmt.Errorf("constraint %q references nonexistent column %q", con.Name, colName)
		}
	}
	if con.Type == ConstraintForeignKey {
		if con.ReferencedTable == "" {
			return fmt.Errorf("foreign key constraint %q is missing referenced_table", con.Name)
		}
		if len(con.ReferencedColumns) == 0 {
			return fmt.Errorf("foreign key constraint %q is missing referenced_columns", con.Name)
		}
	}
	return nil
}

func (db *Database) validateForeignKeys() error {
	for _, t := range db.Tables {
		for _, con := range t.Constraints {
			if con.Type != ConstraintForeignKey {
				continue
			}
			refTable := db.FindTable(con.ReferencedTable)
			if refTable == nil {
				return fmt.Errorf("table %q, constraint %q: references non-existent table %q",
					t.Name, con.Name, con.ReferencedTable)
			}
			for _, refColName := range con.ReferencedColumns {
				if refTable.FindColumn(refColName) == nil {
					return fmt.Errorf("table %q, constraint %q: references non-existent column %q in table %q",
						t.Name, con.Name, refColName, con.ReferencedTable)
				}
			}
			for _, colName := range con.Columns {
				if t.FindColumn(colName) == nil {
					return fmt.Errorf("table %q, constraint %q: references non-existent column %q",
						t.Name, con.Name, colName)
				}
			}
		}
	}
	return nil
}

// synthesizeConstraints generates constraint objects from column-level
// shortcuts (primary_key, unique, check, references).
func (t *Table) synthesizeConstraints() {
	t.synthesizePrimaryKey()
	t.synthesizeUniqueConstraints()
	t.synthesizeCheckConstraints()
	t.synthesizeForeignKeyConstraints()
}

func (t *Table) synthesizePrimaryKey() {
	for _, con := range t.Constraints {
		if con.Type == ConstraintPrimaryKey {
			return
		}
	}

	var pkCols []string
	for _, col := range t.Columns {
		if col.PrimaryKey {
			pkCols = append(pkCols, col.Name)
		}
	}
	if len(pkCols) == 0 {
		return
	}

	name := AutoGenerateConstraintName(ConstraintPrimaryKey, t.Name, pkCols, "")
	t.Constraints = append(t.Constraints, &Constraint{
		Name:    name,
		Type:    ConstraintPrimaryKey,
		Columns: pkCols,
	})
}

func (t *Table) synthesizeUniqueConstraints() {
	for _, col := range t.Columns {
		if !col.Unique {
			continue
		}
		cols := []string{col.Name}
		name := AutoGenerateConstraintName(ConstraintUnique, t.Name, cols, "")
		t.Constraints = append(t.Constraints, &Constraint{
			Name:    name,
			Type:    ConstraintUnique,
			Columns: cols,
		})
	}
}

func (t *Table) synthesizeCheckConstraints() {
	for _, col := range t.Columns {
		if col.Check == "" {
			continue
		}
		cols := []string{col.Name}
		name := AutoGenerateConstraintName(ConstraintCheck, t.Name, cols, "")
		t.Constraints = append(t.Constraints, &Constraint{
			Name:            name,
			Type:            ConstraintCheck,
			CheckExpression: col.Check,
			Enforced:        true,
		})
	}
}

func (t *Table) synthesizeForeignKeyConstraints() {
	for _, col := range t.Columns {
		if col.References == "" {
			continue
		}
		refTable, refCol, ok := ParseReferences(col.References)
		if !ok {
			continue
		}
		cols := []string{col.Name}
		name := AutoGenerateConstraintName(ConstraintForeignKey, t.Name, cols, refTable)
		t.Constraints = append(t.Constraints, &Constraint{
			Name:              name,
			Type:              ConstraintForeignKey,
			Columns:           cols,
			ReferencedTable:   refTable,
			ReferencedColumns: []string{refCol},
			OnDelete:          col.RefOnDelete,
			OnUpdate:          col.RefOnUpdate,
			Enforced:          true,
		})
	}
}
