package core

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

type SchemaDiff struct {
	AddedTables    []*Table
	RemovedTables  []*Table
	ModifiedTables []*TableDiff
}

type TableDiff struct {
	Name               string
	AddedColumns       []*Column
	RemovedColumns     []*Column
	ModifiedColumns    []*ColumnChange
	AddedConstraints   []*Constraint
	RemovedConstraints []*Constraint
	AddedIndexes       []*Index
	RemovedIndexes     []*Index
}

type ColumnChange struct {
	Name string
	Old  *Column
	New  *Column
}

func Diff(oldDB, newDB *Database) *SchemaDiff {
	d := &SchemaDiff{}

	oldTables := map[string]*Table{}
	newTables := map[string]*Table{}

	for _, t := range oldDB.Tables {
		oldTables[strings.ToLower(t.Name)] = t
	}
	for _, t := range newDB.Tables {
		newTables[strings.ToLower(t.Name)] = t
	}

	for name, nt := range newTables {
		ot, ok := oldTables[name]
		if !ok {
			d.AddedTables = append(d.AddedTables, nt)
			continue
		}

		td := compareTable(ot, nt)
		if td != nil {
			d.ModifiedTables = append(d.ModifiedTables, td)
		}
	}

	for name, ot := range oldTables {
		if _, ok := newTables[name]; !ok {
			d.RemovedTables = append(d.RemovedTables, ot)
		}
	}

	sort.Slice(d.AddedTables, func(i, j int) bool {
		return strings.ToLower(d.AddedTables[i].Name) < strings.ToLower(d.AddedTables[j].Name)
	})
	sort.Slice(d.RemovedTables, func(i, j int) bool {
		return strings.ToLower(d.RemovedTables[i].Name) < strings.ToLower(d.RemovedTables[j].Name)
	})
	sort.Slice(d.ModifiedTables, func(i, j int) bool {
		return strings.ToLower(d.ModifiedTables[i].Name) < strings.ToLower(d.ModifiedTables[j].Name)
	})

	return d
}

func compareTable(oldT, newT *Table) *TableDiff {
	td := &TableDiff{Name: newT.Name}

	oldCols := map[string]*Column{}
	newCols := map[string]*Column{}
	for _, c := range oldT.Columns {
		oldCols[strings.ToLower(c.Name)] = c
	}
	for _, c := range newT.Columns {
		newCols[strings.ToLower(c.Name)] = c
	}

	for name, nc := range newCols {
		if oc, ok := oldCols[name]; !ok {
			td.AddedColumns = append(td.AddedColumns, nc)
		} else if !equalColumn(oc, nc) {
			td.ModifiedColumns = append(td.ModifiedColumns, &ColumnChange{Name: nc.Name, Old: oc, New: nc})
		}
	}
	for name, oc := range oldCols {
		if _, ok := newCols[name]; !ok {
			td.RemovedColumns = append(td.RemovedColumns, oc)
		}
	}

	oldCons := map[string]*Constraint{}
	newCons := map[string]*Constraint{}
	for _, c := range oldT.Constraints {
		oldCons[strings.ToLower(c.Name)] = c
	}
	for _, c := range newT.Constraints {
		newCons[strings.ToLower(c.Name)] = c
	}
	for name, nc := range newCons {
		if _, ok := oldCons[name]; !ok {
			td.AddedConstraints = append(td.AddedConstraints, nc)
		}
	}
	for name, oc := range oldCons {
		if _, ok := newCons[name]; !ok {
			td.RemovedConstraints = append(td.RemovedConstraints, oc)
		}
	}

	oldIdx := map[string]*Index{}
	newIdx := map[string]*Index{}
	for _, i := range oldT.Indexes {
		oldIdx[strings.ToLower(i.Name)] = i
	}
	for _, i := range newT.Indexes {
		newIdx[strings.ToLower(i.Name)] = i
	}
	for name, ni := range newIdx {
		if _, ok := oldIdx[name]; !ok {
			td.AddedIndexes = append(td.AddedIndexes, ni)
		}
	}
	for name, oi := range oldIdx {
		if _, ok := newIdx[name]; !ok {
			td.RemovedIndexes = append(td.RemovedIndexes, oi)
		}
	}

	if len(td.AddedColumns) == 0 && len(td.RemovedColumns) == 0 && len(td.ModifiedColumns) == 0 && len(td.AddedConstraints) == 0 && len(td.RemovedConstraints) == 0 && len(td.AddedIndexes) == 0 && len(td.RemovedIndexes) == 0 {
		return nil
	}

	sort.Slice(td.AddedColumns, func(i, j int) bool {
		return strings.ToLower(td.AddedColumns[i].Name) < strings.ToLower(td.AddedColumns[j].Name)
	})
	sort.Slice(td.RemovedColumns, func(i, j int) bool {
		return strings.ToLower(td.RemovedColumns[i].Name) < strings.ToLower(td.RemovedColumns[j].Name)
	})
	sort.Slice(td.ModifiedColumns, func(i, j int) bool {
		return strings.ToLower(td.ModifiedColumns[i].Name) < strings.ToLower(td.ModifiedColumns[j].Name)
	})
	sort.Slice(td.AddedConstraints, func(i, j int) bool {
		return strings.ToLower(td.AddedConstraints[i].Name) < strings.ToLower(td.AddedConstraints[j].Name)
	})
	sort.Slice(td.RemovedConstraints, func(i, j int) bool {
		return strings.ToLower(td.RemovedConstraints[i].Name) < strings.ToLower(td.RemovedConstraints[j].Name)
	})
	sort.Slice(td.AddedIndexes, func(i, j int) bool {
		return strings.ToLower(td.AddedIndexes[i].Name) < strings.ToLower(td.AddedIndexes[j].Name)
	})
	sort.Slice(td.RemovedIndexes, func(i, j int) bool {
		return strings.ToLower(td.RemovedIndexes[i].Name) < strings.ToLower(td.RemovedIndexes[j].Name)
	})

	return td
}

func equalColumn(a, b *Column) bool {
	if strings.ToLower(a.TypeRaw) != strings.ToLower(b.TypeRaw) {
		return false
	}
	if a.Nullable != b.Nullable {
		return false
	}
	if a.PrimaryKey != b.PrimaryKey {
		return false
	}
	if a.AutoIncrement != b.AutoIncrement {
		return false
	}
	var av, bv string
	if a.DefaultValue != nil {
		av = *a.DefaultValue
	}
	if b.DefaultValue != nil {
		bv = *b.DefaultValue
	}
	if av != bv {
		return false
	}
	return true
}

func (d *SchemaDiff) String() string {
	var sb strings.Builder
	if len(d.AddedTables) == 0 && len(d.RemovedTables) == 0 && len(d.ModifiedTables) == 0 {
		return "No differences detected."
	}

	sb.WriteString("Schema differences:\n")

	if len(d.AddedTables) > 0 {
		sb.WriteString("\nAdded tables:\n")
		for _, t := range d.AddedTables {
			sb.WriteString(fmt.Sprintf(" - %s\n", t.Name))
		}
	}
	if len(d.RemovedTables) > 0 {
		sb.WriteString("\nRemoved tables:\n")
		for _, t := range d.RemovedTables {
			sb.WriteString(fmt.Sprintf(" - %s\n", t.Name))
		}
	}
	if len(d.ModifiedTables) > 0 {
		sb.WriteString("\nModified tables:\n")
		for _, mt := range d.ModifiedTables {
			sb.WriteString(fmt.Sprintf("\nTable: %s\n", mt.Name))
			if len(mt.AddedColumns) > 0 {
				sb.WriteString("  Added columns:\n")
				for _, c := range mt.AddedColumns {
					sb.WriteString(fmt.Sprintf("    - %s: %s\n", c.Name, c.TypeRaw))
				}
			}
			if len(mt.RemovedColumns) > 0 {
				sb.WriteString("  Removed columns:\n")
				for _, c := range mt.RemovedColumns {
					sb.WriteString(fmt.Sprintf("    - %s: %s\n", c.Name, c.TypeRaw))
				}
			}
			if len(mt.ModifiedColumns) > 0 {
				sb.WriteString("  Modified columns:\n")
				for _, ch := range mt.ModifiedColumns {
					sb.WriteString(fmt.Sprintf("    - %s:\n", ch.Name))
					sb.WriteString(fmt.Sprintf("      - old: %s (nullable=%v)\n", ch.Old.TypeRaw, ch.Old.Nullable))
					sb.WriteString(fmt.Sprintf("      - new: %s (nullable=%v)\n", ch.New.TypeRaw, ch.New.Nullable))
				}
			}
			if len(mt.AddedConstraints) > 0 {
				sb.WriteString("  Added constraints:\n")
				for _, c := range mt.AddedConstraints {
					sb.WriteString(fmt.Sprintf("    - %s (%s)\n", c.Name, c.Type))
				}
			}
			if len(mt.RemovedConstraints) > 0 {
				sb.WriteString("  Removed constraints:\n")
				for _, c := range mt.RemovedConstraints {
					sb.WriteString(fmt.Sprintf("    - %s (%s)\n", c.Name, c.Type))
				}
			}
			if len(mt.AddedIndexes) > 0 {
				sb.WriteString("  Added indexes:\n")
				for _, idx := range mt.AddedIndexes {
					sb.WriteString(fmt.Sprintf("    - %s (%v)\n", idx.Name, idx.Columns))
				}
			}
			if len(mt.RemovedIndexes) > 0 {
				sb.WriteString("  Removed indexes:\n")
				for _, idx := range mt.RemovedIndexes {
					sb.WriteString(fmt.Sprintf("    - %s (%v)\n", idx.Name, idx.Columns))
				}
			}
		}
	}

	return sb.String()
}

func (d *SchemaDiff) SaveToFile(path string) error {
	return os.WriteFile(path, []byte(d.String()), 0644)
}
