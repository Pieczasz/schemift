package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
	"smf/internal/diff"
)

func TestRollbackSuggestionsAddedTable(t *testing.T) {
	g := NewMySQLGenerator()

	table := &core.Table{
		Name: "users",
		Columns: []*core.Column{
			{Name: "id", TypeRaw: "INT", Nullable: false},
		},
	}

	schemaDiff := &diff.SchemaDiff{
		AddedTables: []*core.Table{table},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)

	require.Len(t, rollbacks, 1)
	assert.Contains(t, rollbacks[0], "DROP TABLE")
	assert.Contains(t, rollbacks[0], "`users`")
}

func TestRollbackSuggestionsAddedTableNil(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		AddedTables: []*core.Table{nil},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)
	assert.Empty(t, rollbacks)
}

func TestRollbackSuggestionsRemovedTable(t *testing.T) {
	g := NewMySQLGenerator()

	table := &core.Table{
		Name: "users",
	}

	schemaDiff := &diff.SchemaDiff{
		RemovedTables: []*core.Table{table},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)

	require.Len(t, rollbacks, 1)
	assert.Contains(t, rollbacks[0], "-- cannot auto-rollback DROP TABLE")
	assert.Contains(t, rollbacks[0], "`users`")
}

func TestRollbackSuggestionsRemovedTableNil(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		RemovedTables: []*core.Table{nil},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)
	assert.Empty(t, rollbacks)
}

func TestRollbackSuggestionsAddedColumn(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "users",
				AddedColumns: []*core.Column{
					{Name: "email", TypeRaw: "VARCHAR(255)", Nullable: true},
				},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)

	require.Len(t, rollbacks, 1)
	assert.Contains(t, rollbacks[0], "ALTER TABLE")
	assert.Contains(t, rollbacks[0], "DROP COLUMN")
	assert.Contains(t, rollbacks[0], "`email`")
}

func TestRollbackSuggestionsAddedColumnNil(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name:         "users",
				AddedColumns: []*core.Column{nil},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)
	assert.Empty(t, rollbacks)
}

func TestRollbackSuggestionsRemovedColumn(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "users",
				RemovedColumns: []*core.Column{
					{Name: "email", TypeRaw: "VARCHAR(255)", Nullable: true},
				},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)

	require.Len(t, rollbacks, 1)
	assert.Contains(t, rollbacks[0], "ALTER TABLE")
	assert.Contains(t, rollbacks[0], "ADD COLUMN")
	assert.Contains(t, rollbacks[0], "`email`")
}

func TestRollbackSuggestionsRemovedColumnNil(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name:           "users",
				RemovedColumns: []*core.Column{nil},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)
	assert.Empty(t, rollbacks)
}

func TestRollbackSuggestionsModifiedColumn(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "users",
				ModifiedColumns: []*diff.ColumnChange{
					{
						Old: &core.Column{Name: "email", TypeRaw: "VARCHAR(100)", Nullable: true},
						New: &core.Column{Name: "email", TypeRaw: "VARCHAR(255)", Nullable: true},
					},
				},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)

	require.Len(t, rollbacks, 1)
	assert.Contains(t, rollbacks[0], "ALTER TABLE")
	assert.Contains(t, rollbacks[0], "MODIFY COLUMN")
	assert.Contains(t, rollbacks[0], "VARCHAR(100)")
}

func TestRollbackSuggestionsModifiedColumnNil(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name:            "users",
				ModifiedColumns: []*diff.ColumnChange{nil},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)
	assert.Empty(t, rollbacks)
}

func TestRollbackSuggestionsModifiedColumnNilOld(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "users",
				ModifiedColumns: []*diff.ColumnChange{
					{Old: nil, New: &core.Column{Name: "email", TypeRaw: "VARCHAR(255)"}},
				},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)
	assert.Empty(t, rollbacks)
}

func TestRollbackSuggestionsAddedConstraint(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "users",
				AddedConstraints: []*core.Constraint{
					{Name: "pk_users", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}},
				},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)

	require.Len(t, rollbacks, 1)
	assert.Contains(t, rollbacks[0], "DROP PRIMARY KEY")
}

func TestRollbackSuggestionsRemovedConstraint(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "users",
				RemovedConstraints: []*core.Constraint{
					{Name: "pk_users", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}},
				},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)

	require.Len(t, rollbacks, 1)
	assert.Contains(t, rollbacks[0], "ADD PRIMARY KEY")
}

func TestRollbackSuggestionsModifiedConstraint(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "users",
				ModifiedConstraints: []*diff.ConstraintChange{
					{
						Old: &core.Constraint{Name: "uq_email", Type: core.ConstraintUnique, Columns: []string{"email"}},
						New: &core.Constraint{Name: "uq_email", Type: core.ConstraintUnique, Columns: []string{"email", "name"}},
					},
				},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)

	require.Len(t, rollbacks, 2)
	assert.Contains(t, rollbacks[0], "DROP INDEX")
	assert.Contains(t, rollbacks[1], "ADD CONSTRAINT")
}

func TestRollbackSuggestionsModifiedConstraintNil(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name:                "users",
				ModifiedConstraints: []*diff.ConstraintChange{nil},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)
	assert.Empty(t, rollbacks)
}

func TestRollbackSuggestionsAddedIndex(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "users",
				AddedIndexes: []*core.Index{
					{Name: "idx_email", Columns: []core.IndexColumn{{Name: "email"}}},
				},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)

	require.Len(t, rollbacks, 1)
	assert.Contains(t, rollbacks[0], "DROP INDEX")
	assert.Contains(t, rollbacks[0], "`idx_email`")
}

func TestRollbackSuggestionsAddedIndexNilOrEmpty(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "users",
				AddedIndexes: []*core.Index{
					nil,
					{Name: "", Columns: []core.IndexColumn{{Name: "email"}}},
					{Name: "   ", Columns: []core.IndexColumn{{Name: "email"}}},
				},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)
	assert.Empty(t, rollbacks)
}

func TestRollbackSuggestionsRemovedIndex(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "users",
				RemovedIndexes: []*core.Index{
					{Name: "idx_email", Columns: []core.IndexColumn{{Name: "email"}}},
				},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)

	require.Len(t, rollbacks, 1)
	assert.Contains(t, rollbacks[0], "CREATE INDEX")
	assert.Contains(t, rollbacks[0], "`idx_email`")
}

func TestRollbackSuggestionsRemovedIndexNil(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name:           "users",
				RemovedIndexes: []*core.Index{nil},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)
	assert.Empty(t, rollbacks)
}

func TestRollbackSuggestionsModifiedIndex(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "users",
				ModifiedIndexes: []*diff.IndexChange{
					{
						Old: &core.Index{Name: "idx_email", Columns: []core.IndexColumn{{Name: "email"}}},
						New: &core.Index{Name: "idx_email", Columns: []core.IndexColumn{{Name: "email"}, {Name: "name"}}},
					},
				},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)

	require.Len(t, rollbacks, 2)
	assert.Contains(t, rollbacks[0], "DROP INDEX")
	assert.Contains(t, rollbacks[1], "CREATE INDEX")
}

func TestRollbackSuggestionsModifiedIndexNil(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name:            "users",
				ModifiedIndexes: []*diff.IndexChange{nil},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)
	assert.Empty(t, rollbacks)
}

func TestRollbackSuggestionsModifiedIndexNewNilName(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "users",
				ModifiedIndexes: []*diff.IndexChange{
					{
						Old: &core.Index{Name: "idx_email", Columns: []core.IndexColumn{{Name: "email"}}},
						New: &core.Index{Name: "", Columns: []core.IndexColumn{{Name: "email"}}},
					},
				},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)

	require.Len(t, rollbacks, 1)
	assert.Contains(t, rollbacks[0], "CREATE INDEX")
}

func TestRollbackSuggestionsModifiedOptions(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "users",
				ModifiedOptions: []*diff.TableOptionChange{
					{Name: "ENGINE", Old: "InnoDB", New: "MyISAM"},
				},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)

	require.Len(t, rollbacks, 1)
	assert.Contains(t, rollbacks[0], "ALTER TABLE")
	assert.Contains(t, rollbacks[0], "ENGINE=InnoDB")
}

func TestRollbackSuggestionsModifiedOptionsNil(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name:            "users",
				ModifiedOptions: []*diff.TableOptionChange{nil},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)
	assert.Empty(t, rollbacks)
}

func TestRollbackSuggestionsModifiedTableNil(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{nil},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)
	assert.Empty(t, rollbacks)
}

func TestRollbackSuggestionsEmptyDiff(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{}

	rollbacks := g.rollbackSuggestions(schemaDiff)
	assert.Empty(t, rollbacks)
}

func TestRollbackSuggestionsRemovedIndexEmptyName(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "users",
				RemovedIndexes: []*core.Index{
					{Name: "", Columns: []core.IndexColumn{{Name: "email"}}},
				},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)

	assert.Empty(t, rollbacks)
}

func TestRollbackSuggestionsModifiedIndexOldEmptyName(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "users",
				ModifiedIndexes: []*diff.IndexChange{
					{
						Old: &core.Index{Name: "", Columns: []core.IndexColumn{{Name: "email"}}},
						New: &core.Index{Name: "idx_email", Columns: []core.IndexColumn{{Name: "email"}}},
					},
				},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)

	require.Len(t, rollbacks, 1)
	assert.Contains(t, rollbacks[0], "DROP INDEX")
	assert.Contains(t, rollbacks[0], "`idx_email`")
}

func TestRollbackSuggestionsComplexScenario(t *testing.T) {
	g := NewMySQLGenerator()

	schemaDiff := &diff.SchemaDiff{
		AddedTables: []*core.Table{
			{Name: "orders", Columns: []*core.Column{{Name: "id", TypeRaw: "INT"}}},
		},
		RemovedTables: []*core.Table{
			{Name: "legacy_data"},
		},
		ModifiedTables: []*diff.TableDiff{
			{
				Name: "users",
				AddedColumns: []*core.Column{
					{Name: "phone", TypeRaw: "VARCHAR(20)", Nullable: true},
				},
				RemovedColumns: []*core.Column{
					{Name: "fax", TypeRaw: "VARCHAR(20)", Nullable: true},
				},
				AddedIndexes: []*core.Index{
					{Name: "idx_phone", Columns: []core.IndexColumn{{Name: "phone"}}},
				},
			},
		},
	}

	rollbacks := g.rollbackSuggestions(schemaDiff)

	assert.GreaterOrEqual(t, len(rollbacks), 4)
}
