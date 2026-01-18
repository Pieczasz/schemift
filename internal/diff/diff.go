// Package diff provides functionality to compare and generate schema diffs between two schema dumps.
// It also includes breaking changes detection.
package diff

import (
	"smf/internal/core"
)

const (
	renameDetectionScoreThreshold = 9
	renameSharedTokenMinLen       = 3
)

// SchemaDiff represents the differences between two schema dumps.
type SchemaDiff struct {
	AddedTables    []*core.Table
	RemovedTables  []*core.Table
	ModifiedTables []*TableDiff
}

// TableDiff represents the differences between two tables.
type TableDiff struct {
	Name                string
	AddedColumns        []*core.Column
	RemovedColumns      []*core.Column
	RenamedColumns      []*ColumnRename
	ModifiedColumns     []*ColumnChange
	AddedConstraints    []*core.Constraint
	RemovedConstraints  []*core.Constraint
	ModifiedConstraints []*ConstraintChange
	AddedIndexes        []*core.Index
	RemovedIndexes      []*core.Index
	ModifiedIndexes     []*IndexChange
	ModifiedOptions     []*TableOptionChange
}

// ColumnChange represents the differences between two columns.
type ColumnChange struct {
	Name    string
	Old     *core.Column
	New     *core.Column
	Changes []*FieldChange
}

// ColumnRename represents the score of a column rename detection.
type ColumnRename struct {
	Old   *core.Column
	New   *core.Column
	Score int
}

// ConstraintChange represents the constraint difference between old table and new table.
type ConstraintChange struct {
	Name          string
	Old           *core.Constraint
	New           *core.Constraint
	Changes       []*FieldChange
	RebuildOnly   bool
	RebuildReason string
}

// IndexChange represents the differences between indexes of old table and new table.
type IndexChange struct {
	Name    string
	Old     *core.Index
	New     *core.Index
	Changes []*FieldChange
}

// FieldChange represents the differences between two fields.
type FieldChange struct {
	Field string
	Old   string
	New   string
}

// TableOptionChange represents the differences between two table options.
type TableOptionChange struct {
	Name string
	Old  string
	New  string
}

// Diff compares two database dumps and returns a SchemaDiff object.
func Diff(oldDB, newDB *core.Database) *SchemaDiff {
	d := &SchemaDiff{}

	oldTables := mapByLowerName(oldDB.Tables, func(t *core.Table) string { return t.Name })
	newTables := mapByLowerName(newDB.Tables, func(t *core.Table) string { return t.Name })

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

	sortByNameCI(d.AddedTables, func(t *core.Table) string { return t.Name })
	sortByNameCI(d.RemovedTables, func(t *core.Table) string { return t.Name })
	sortByNameCI(d.ModifiedTables, func(td *TableDiff) string { return td.Name })

	return d
}
