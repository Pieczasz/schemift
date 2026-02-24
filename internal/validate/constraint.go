package validate

import (
	"fmt"

	"smf/internal/core"
)

func Constraints(t *core.Table) error {
	if err := ConstraintNames(t); err != nil {
		return err
	}
	return ConstraintColumns(t)
}

func ConstraintNames(t *core.Table) error {
	seen := make(map[string]bool, len(t.Constraints))
	for _, con := range t.Constraints {
		if con.Name == "" {
			continue
		}
		if err := Name(con.Name, nil, nil, false); err != nil {
			return fmt.Errorf("constraint %q: %w", con.Name, err)
		}
		if seen[con.Name] {
			return fmt.Errorf("duplicate constraint name %q", con.Name)
		}
		seen[con.Name] = true
	}
	return nil
}

func ConstraintColumns(t *core.Table) error {
	for _, con := range t.Constraints {
		if err := SingleConstraintColumns(t, con); err != nil {
			return err
		}
	}
	return nil
}

func SingleConstraintColumns(t *core.Table, con *core.Constraint) error {
	if con.Type == core.ConstraintCheck {
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
	if con.Type == core.ConstraintForeignKey {
		if con.ReferencedTable == "" {
			return fmt.Errorf("foreign key constraint %q is missing referenced_table", con.Name)
		}
		if len(con.ReferencedColumns) == 0 {
			return fmt.Errorf("foreign key constraint %q is missing referenced_columns", con.Name)
		}
	}
	return nil
}

func ForeignKeys(tables []*core.Table) error {
	for _, t := range tables {
		for _, con := range t.Constraints {
			if con.Type != core.ConstraintForeignKey {
				continue
			}
			refTable := FindTable(tables, con.ReferencedTable)
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

func FindTable(tables []*core.Table, name string) *core.Table {
	for _, t := range tables {
		if t.Name == name {
			return t
		}
	}
	return nil
}

func Synthesize(t *core.Table) {
	SynthesizePrimaryKey(t)
	SynthesizeUniqueConstraints(t)
	SynthesizeCheckConstraints(t)
	SynthesizeForeignKeyConstraints(t)
}

func SynthesizePrimaryKey(t *core.Table) {
	for _, con := range t.Constraints {
		if con.Type == core.ConstraintPrimaryKey {
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

	name := core.AutoGenerateConstraintName(core.ConstraintPrimaryKey, t.Name, pkCols, "")
	t.Constraints = append(t.Constraints, &core.Constraint{
		Name:    name,
		Type:    core.ConstraintPrimaryKey,
		Columns: pkCols,
	})
}

func SynthesizeUniqueConstraints(t *core.Table) {
	for _, col := range t.Columns {
		if !col.Unique {
			continue
		}
		cols := []string{col.Name}
		name := core.AutoGenerateConstraintName(core.ConstraintUnique, t.Name, cols, "")
		t.Constraints = append(t.Constraints, &core.Constraint{
			Name:    name,
			Type:    core.ConstraintUnique,
			Columns: cols,
		})
	}
}

func SynthesizeCheckConstraints(t *core.Table) {
	for _, col := range t.Columns {
		if col.Check == "" {
			continue
		}
		cols := []string{col.Name}
		name := core.AutoGenerateConstraintName(core.ConstraintCheck, t.Name, cols, "")
		t.Constraints = append(t.Constraints, &core.Constraint{
			Name:            name,
			Type:            core.ConstraintCheck,
			CheckExpression: col.Check,
			Enforced:        true,
		})
	}
}

func SynthesizeForeignKeyConstraints(t *core.Table) {
	for _, col := range t.Columns {
		if col.References == "" {
			continue
		}
		refTable, refCol, ok := core.ParseReferences(col.References)
		if !ok {
			continue
		}
		cols := []string{col.Name}
		name := core.AutoGenerateConstraintName(core.ConstraintForeignKey, t.Name, cols, refTable)
		t.Constraints = append(t.Constraints, &core.Constraint{
			Name:              name,
			Type:              core.ConstraintForeignKey,
			Columns:           cols,
			ReferencedTable:   refTable,
			ReferencedColumns: []string{refCol},
			OnDelete:          col.RefOnDelete,
			OnUpdate:          col.RefOnUpdate,
			Enforced:          true,
		})
	}
}
