package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
	"smf/internal/dialect"
	"smf/internal/diff"
)

func TestAlterTableResultAdd(t *testing.T) {
	result := &AlterTableResult{}

	result.Add("ALTER TABLE users ADD COLUMN email VARCHAR(255);", "ALTER TABLE users DROP COLUMN email;")

	require.Len(t, result.Statements, 1)
	require.Len(t, result.Rollback, 1)
	assert.Equal(t, "ALTER TABLE users ADD COLUMN email VARCHAR(255);", result.Statements[0])
	assert.Equal(t, "ALTER TABLE users DROP COLUMN email;", result.Rollback[0])
}

func TestAlterTableResultAddEmptyRollback(t *testing.T) {
	result := &AlterTableResult{}

	result.Add("ALTER TABLE users ADD COLUMN email VARCHAR(255);", "")

	require.Len(t, result.Statements, 1)
	assert.Empty(t, result.Rollback)
}

func TestAlterTableResultAddWhitespaceRollback(t *testing.T) {
	result := &AlterTableResult{}

	result.Add("ALTER TABLE users ADD COLUMN email VARCHAR(255);", "   ")

	require.Len(t, result.Statements, 1)
	assert.Empty(t, result.Rollback)
}

func TestAlterTableResultAddFK(t *testing.T) {
	result := &AlterTableResult{}

	result.AddFK("ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id);", "ALTER TABLE orders DROP FOREIGN KEY fk_user;")

	require.Len(t, result.FKStatements, 1)
	require.Len(t, result.FKRollback, 1)
	assert.Equal(t, "ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id);", result.FKStatements[0])
	assert.Equal(t, "ALTER TABLE orders DROP FOREIGN KEY fk_user;", result.FKRollback[0])
}

func TestAlterTableResultAddFKEmptyRollback(t *testing.T) {
	result := &AlterTableResult{}

	result.AddFK("ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id);", "")

	require.Len(t, result.FKStatements, 1)
	assert.Empty(t, result.FKRollback)
}

func TestAlterTableResultAllStatements(t *testing.T) {
	result := &AlterTableResult{
		Statements:   []string{"stmt1", "stmt2"},
		FKStatements: []string{"fk1", "fk2"},
	}

	all := result.AllStatements()

	require.Len(t, all, 4)
	assert.Equal(t, []string{"stmt1", "stmt2", "fk1", "fk2"}, all)
}

func TestAlterTableResultAllStatementsEmpty(t *testing.T) {
	result := &AlterTableResult{}

	all := result.AllStatements()

	assert.Empty(t, all)
}

func TestMigrationRecommendationsColumnRename(t *testing.T) {
	bc := diff.BreakingChange{
		Table:       "users",
		Object:      "name",
		Description: "column rename detected",
	}

	recs := migrationRecommendations(bc)

	require.Len(t, recs, 1)
	assert.Contains(t, recs[0], "Data migration tip")
	assert.Contains(t, recs[0], "rename")
}

func TestMigrationRecommendationsBecomesNotNull(t *testing.T) {
	bc := diff.BreakingChange{
		Table:       "users",
		Object:      "email",
		Description: "becomes not null",
	}

	recs := migrationRecommendations(bc)

	require.Len(t, recs, 1)
	assert.Contains(t, recs[0], "backfill")
}

func TestMigrationRecommendationsAddingNotNullWithoutDefault(t *testing.T) {
	bc := diff.BreakingChange{
		Table:       "users",
		Object:      "phone",
		Description: "adding not null column without default",
	}

	recs := migrationRecommendations(bc)

	require.Len(t, recs, 1)
	assert.Contains(t, recs[0], "add")
	assert.Contains(t, recs[0], "NULL first")
}

func TestMigrationRecommendationsTypeChanges(t *testing.T) {
	bc := diff.BreakingChange{
		Table:       "users",
		Object:      "age",
		Description: "type changes",
	}

	recs := migrationRecommendations(bc)

	require.Len(t, recs, 1)
	assert.Contains(t, recs[0], "cast/backfill")
}

func TestMigrationRecommendationsLengthShrinks(t *testing.T) {
	bc := diff.BreakingChange{
		Table:       "users",
		Object:      "name",
		Description: "length shrinks",
	}

	recs := migrationRecommendations(bc)

	require.Len(t, recs, 1)
	assert.Contains(t, recs[0], "max length")
}

func TestMigrationRecommendationsTableDropped(t *testing.T) {
	bc := diff.BreakingChange{
		Table:       "old_table",
		Object:      "",
		Description: "table will be dropped",
	}

	recs := migrationRecommendations(bc)

	require.Len(t, recs, 1)
	assert.Contains(t, recs[0], "Safety tip")
	assert.Contains(t, recs[0], "backup")
}

func TestMigrationRecommendationsColumnDropped(t *testing.T) {
	bc := diff.BreakingChange{
		Table:       "users",
		Object:      "old_column",
		Description: "column will be dropped",
	}

	recs := migrationRecommendations(bc)

	require.Len(t, recs, 1)
	assert.Contains(t, recs[0], "Safety tip")
	assert.Contains(t, recs[0], "backup")
}

func TestMigrationRecommendationsNoMatch(t *testing.T) {
	bc := diff.BreakingChange{
		Table:       "users",
		Object:      "email",
		Description: "some other change",
	}

	recs := migrationRecommendations(bc)

	assert.Empty(t, recs)
}

func TestGenerateAlterTableNilOptions(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		AddedColumns: []*core.Column{
			{Name: "email", TypeRaw: "VARCHAR(255)", Nullable: true},
		},
	}

	result := g.generateAlterTable(td, nil)

	require.NotNil(t, result)
	require.NotEmpty(t, result.Statements)
}

func TestGenerateConstraintDropsModified(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		ModifiedConstraints: []*diff.ConstraintChange{
			{
				Old: &core.Constraint{Name: "uq_email", Type: core.ConstraintUnique, Columns: []string{"email"}},
				New: &core.Constraint{Name: "uq_email", Type: core.ConstraintUnique, Columns: []string{"email", "name"}},
			},
		},
	}

	result := &AlterTableResult{}
	g.generateConstraintDrops(td, "`users`", result)

	require.NotEmpty(t, result.Statements)
	assert.Contains(t, result.Statements[0], "DROP INDEX")
}

func TestGenerateConstraintDropsModifiedNil(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name:                "users",
		ModifiedConstraints: []*diff.ConstraintChange{nil},
	}

	result := &AlterTableResult{}
	g.generateConstraintDrops(td, "`users`", result)

	assert.Empty(t, result.Statements)
}

func TestGenerateConstraintDropsModifiedNilOld(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		ModifiedConstraints: []*diff.ConstraintChange{
			{Old: nil, New: &core.Constraint{Name: "uq_email", Type: core.ConstraintUnique}},
		},
	}

	result := &AlterTableResult{}
	g.generateConstraintDrops(td, "`users`", result)

	assert.Empty(t, result.Statements)
}

func TestGenerateConstraintDropsRemoved(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		RemovedConstraints: []*core.Constraint{
			{Name: "uq_email", Type: core.ConstraintUnique, Columns: []string{"email"}},
		},
	}

	result := &AlterTableResult{}
	g.generateConstraintDrops(td, "`users`", result)

	require.NotEmpty(t, result.Statements)
	assert.Contains(t, result.Statements[0], "DROP INDEX")
}

func TestGenerateConstraintDropsRemovedNil(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name:               "users",
		RemovedConstraints: []*core.Constraint{nil},
	}

	result := &AlterTableResult{}
	g.generateConstraintDrops(td, "`users`", result)

	assert.Empty(t, result.Statements)
}

func TestGenerateIndexDropsModified(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		ModifiedIndexes: []*diff.IndexChange{
			{
				Old: &core.Index{Name: "idx_email", Columns: []core.IndexColumn{{Name: "email"}}},
				New: &core.Index{Name: "idx_email", Columns: []core.IndexColumn{{Name: "email"}, {Name: "name"}}},
			},
		},
	}

	result := &AlterTableResult{}
	g.generateIndexDrops(td, "`users`", result)

	require.NotEmpty(t, result.Statements)
	assert.Contains(t, result.Statements[0], "DROP INDEX")
}

func TestGenerateIndexDropsModifiedNil(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name:            "users",
		ModifiedIndexes: []*diff.IndexChange{nil},
	}

	result := &AlterTableResult{}
	g.generateIndexDrops(td, "`users`", result)

	assert.Empty(t, result.Statements)
}

func TestGenerateIndexDropsModifiedNilOld(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		ModifiedIndexes: []*diff.IndexChange{
			{Old: nil, New: &core.Index{Name: "idx_email"}},
		},
	}

	result := &AlterTableResult{}
	g.generateIndexDrops(td, "`users`", result)

	assert.Empty(t, result.Statements)
}

func TestGenerateIndexDropsModifiedEmptyName(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		ModifiedIndexes: []*diff.IndexChange{
			{
				Old: &core.Index{Name: "", Columns: []core.IndexColumn{{Name: "email"}}},
				New: &core.Index{Name: "idx_email", Columns: []core.IndexColumn{{Name: "email"}}},
			},
		},
	}

	result := &AlterTableResult{}
	g.generateIndexDrops(td, "`users`", result)

	assert.Empty(t, result.Statements)
}

func TestGenerateIndexDropsRemoved(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		RemovedIndexes: []*core.Index{
			{Name: "idx_email", Columns: []core.IndexColumn{{Name: "email"}}},
		},
	}

	result := &AlterTableResult{}
	g.generateIndexDrops(td, "`users`", result)

	require.NotEmpty(t, result.Statements)
	assert.Contains(t, result.Statements[0], "DROP INDEX")
}

func TestGenerateIndexDropsRemovedNil(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name:           "users",
		RemovedIndexes: []*core.Index{nil},
	}

	result := &AlterTableResult{}
	g.generateIndexDrops(td, "`users`", result)

	assert.Empty(t, result.Statements)
}

func TestGenerateIndexDropsRemovedEmptyName(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		RemovedIndexes: []*core.Index{
			{Name: "", Columns: []core.IndexColumn{{Name: "email"}}},
		},
	}

	result := &AlterTableResult{}
	g.generateIndexDrops(td, "`users`", result)

	assert.Empty(t, result.Statements)
}

func TestGenerateIndexCreatesModified(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		ModifiedIndexes: []*diff.IndexChange{
			{
				Old: &core.Index{Name: "idx_email", Columns: []core.IndexColumn{{Name: "email"}}},
				New: &core.Index{Name: "idx_email", Columns: []core.IndexColumn{{Name: "email"}, {Name: "name"}}},
			},
		},
	}

	result := &AlterTableResult{}
	g.generateIndexCreates(td, "`users`", result)

	require.NotEmpty(t, result.Statements)
	assert.Contains(t, result.Statements[0], "CREATE INDEX")
}

func TestGenerateIndexCreatesModifiedNil(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name:            "users",
		ModifiedIndexes: []*diff.IndexChange{nil},
	}

	result := &AlterTableResult{}
	g.generateIndexCreates(td, "`users`", result)

	assert.Empty(t, result.Statements)
}

func TestGenerateIndexCreatesModifiedNilNew(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		ModifiedIndexes: []*diff.IndexChange{
			{Old: &core.Index{Name: "idx_email"}, New: nil},
		},
	}

	result := &AlterTableResult{}
	g.generateIndexCreates(td, "`users`", result)

	assert.Empty(t, result.Statements)
}

func TestGenerateIndexCreatesAdded(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		AddedIndexes: []*core.Index{
			{Name: "idx_email", Columns: []core.IndexColumn{{Name: "email"}}},
		},
	}

	result := &AlterTableResult{}
	g.generateIndexCreates(td, "`users`", result)

	require.NotEmpty(t, result.Statements)
	assert.Contains(t, result.Statements[0], "CREATE INDEX")
}

func TestGenerateIndexCreatesAddedNil(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name:         "users",
		AddedIndexes: []*core.Index{nil},
	}

	result := &AlterTableResult{}
	g.generateIndexCreates(td, "`users`", result)

	assert.Empty(t, result.Statements)
}

func TestGenerateConstraintAddsModified(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		ModifiedConstraints: []*diff.ConstraintChange{
			{
				Old: &core.Constraint{Name: "uq_email", Type: core.ConstraintUnique, Columns: []string{"email"}},
				New: &core.Constraint{Name: "uq_email", Type: core.ConstraintUnique, Columns: []string{"email", "name"}},
			},
		},
	}

	result := &AlterTableResult{}
	g.generateConstraintAdds(td, "`users`", result)

	require.NotEmpty(t, result.Statements)
	assert.Contains(t, result.Statements[0], "ADD CONSTRAINT")
}

func TestGenerateConstraintAddsModifiedNil(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name:                "users",
		ModifiedConstraints: []*diff.ConstraintChange{nil},
	}

	result := &AlterTableResult{}
	g.generateConstraintAdds(td, "`users`", result)

	assert.Empty(t, result.Statements)
}

func TestGenerateConstraintAddsModifiedNilNew(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		ModifiedConstraints: []*diff.ConstraintChange{
			{Old: &core.Constraint{Name: "uq_email", Type: core.ConstraintUnique}, New: nil},
		},
	}

	result := &AlterTableResult{}
	g.generateConstraintAdds(td, "`users`", result)

	assert.Empty(t, result.Statements)
}

func TestGenerateConstraintAddsModifiedFK(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "orders",
		ModifiedConstraints: []*diff.ConstraintChange{
			{
				Old: &core.Constraint{Name: "fk_user", Type: core.ConstraintForeignKey, Columns: []string{"user_id"}, ReferencedTable: "users", ReferencedColumns: []string{"id"}},
				New: &core.Constraint{Name: "fk_user", Type: core.ConstraintForeignKey, Columns: []string{"user_id"}, ReferencedTable: "users", ReferencedColumns: []string{"user_id"}},
			},
		},
	}

	result := &AlterTableResult{}
	g.generateConstraintAdds(td, "`orders`", result)

	require.NotEmpty(t, result.FKStatements)
	assert.Contains(t, result.FKStatements[0], "FOREIGN KEY")
}

func TestGenerateConstraintAddsAdded(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		AddedConstraints: []*core.Constraint{
			{Name: "uq_email", Type: core.ConstraintUnique, Columns: []string{"email"}},
		},
	}

	result := &AlterTableResult{}
	g.generateConstraintAdds(td, "`users`", result)

	require.NotEmpty(t, result.Statements)
	assert.Contains(t, result.Statements[0], "ADD CONSTRAINT")
}

func TestGenerateConstraintAddsAddedNil(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name:             "users",
		AddedConstraints: []*core.Constraint{nil},
	}

	result := &AlterTableResult{}
	g.generateConstraintAdds(td, "`users`", result)

	assert.Empty(t, result.Statements)
}

func TestGenerateConstraintAddsAddedFK(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "orders",
		AddedConstraints: []*core.Constraint{
			{Name: "fk_user", Type: core.ConstraintForeignKey, Columns: []string{"user_id"}, ReferencedTable: "users", ReferencedColumns: []string{"id"}},
		},
	}

	result := &AlterTableResult{}
	g.generateConstraintAdds(td, "`orders`", result)

	require.NotEmpty(t, result.FKStatements)
	assert.Contains(t, result.FKStatements[0], "FOREIGN KEY")
}

func TestGenerateColumnChangesRenamed(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		RenamedColumns: []*diff.ColumnRename{
			{
				Old: &core.Column{Name: "name", TypeRaw: "VARCHAR(100)", Nullable: true},
				New: &core.Column{Name: "full_name", TypeRaw: "VARCHAR(100)", Nullable: true},
			},
		},
	}

	opts := dialect.DefaultMigrationOptions(dialect.MySQL)
	result := &AlterTableResult{}
	g.generateColumnChanges(td, "`users`", &opts, result)

	require.NotEmpty(t, result.Statements)
	assert.Contains(t, result.Statements[0], "CHANGE COLUMN")
}

func TestGenerateColumnChangesRenamedNil(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name:           "users",
		RenamedColumns: []*diff.ColumnRename{nil},
	}

	opts := dialect.DefaultMigrationOptions(dialect.MySQL)
	result := &AlterTableResult{}
	g.generateColumnChanges(td, "`users`", &opts, result)

	assert.Empty(t, result.Statements)
}

func TestGenerateColumnChangesRenamedNilOld(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		RenamedColumns: []*diff.ColumnRename{
			{Old: nil, New: &core.Column{Name: "full_name", TypeRaw: "VARCHAR(100)"}},
		},
	}

	opts := dialect.DefaultMigrationOptions(dialect.MySQL)
	result := &AlterTableResult{}
	g.generateColumnChanges(td, "`users`", &opts, result)

	assert.Empty(t, result.Statements)
}

func TestGenerateColumnChangesRenamedNilNew(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		RenamedColumns: []*diff.ColumnRename{
			{Old: &core.Column{Name: "name", TypeRaw: "VARCHAR(100)"}, New: nil},
		},
	}

	opts := dialect.DefaultMigrationOptions(dialect.MySQL)
	result := &AlterTableResult{}
	g.generateColumnChanges(td, "`users`", &opts, result)

	assert.Empty(t, result.Statements)
}

func TestGenerateColumnChangesAdded(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		AddedColumns: []*core.Column{
			{Name: "email", TypeRaw: "VARCHAR(255)", Nullable: true},
		},
	}

	opts := dialect.DefaultMigrationOptions(dialect.MySQL)
	result := &AlterTableResult{}
	g.generateColumnChanges(td, "`users`", &opts, result)

	require.NotEmpty(t, result.Statements)
	assert.Contains(t, result.Statements[0], "ADD COLUMN")
}

func TestGenerateColumnChangesAddedNil(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name:         "users",
		AddedColumns: []*core.Column{nil},
	}

	opts := dialect.DefaultMigrationOptions(dialect.MySQL)
	result := &AlterTableResult{}
	g.generateColumnChanges(td, "`users`", &opts, result)

	assert.Empty(t, result.Statements)
}

func TestGenerateColumnChangesModified(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		ModifiedColumns: []*diff.ColumnChange{
			{
				Old: &core.Column{Name: "name", TypeRaw: "VARCHAR(100)", Nullable: true},
				New: &core.Column{Name: "name", TypeRaw: "VARCHAR(255)", Nullable: false},
			},
		},
	}

	opts := dialect.DefaultMigrationOptions(dialect.MySQL)
	result := &AlterTableResult{}
	g.generateColumnChanges(td, "`users`", &opts, result)

	require.NotEmpty(t, result.Statements)
	assert.Contains(t, result.Statements[0], "MODIFY COLUMN")
}

func TestGenerateColumnChangesModifiedNil(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name:            "users",
		ModifiedColumns: []*diff.ColumnChange{nil},
	}

	opts := dialect.DefaultMigrationOptions(dialect.MySQL)
	result := &AlterTableResult{}
	g.generateColumnChanges(td, "`users`", &opts, result)

	assert.Empty(t, result.Statements)
}

func TestGenerateColumnChangesModifiedNilNew(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		ModifiedColumns: []*diff.ColumnChange{
			{Old: &core.Column{Name: "name", TypeRaw: "VARCHAR(100)"}, New: nil},
		},
	}

	opts := dialect.DefaultMigrationOptions(dialect.MySQL)
	result := &AlterTableResult{}
	g.generateColumnChanges(td, "`users`", &opts, result)

	assert.Empty(t, result.Statements)
}

func TestGenerateColumnChangesModifiedNilOld(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		ModifiedColumns: []*diff.ColumnChange{
			{Old: nil, New: &core.Column{Name: "name", TypeRaw: "VARCHAR(255)"}},
		},
	}

	opts := dialect.DefaultMigrationOptions(dialect.MySQL)
	result := &AlterTableResult{}
	g.generateColumnChanges(td, "`users`", &opts, result)

	assert.Empty(t, result.Statements)
}

func TestGenerateColumnRemovalsUnsafe(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		RemovedColumns: []*core.Column{
			{Name: "old_column", TypeRaw: "VARCHAR(100)", Nullable: true},
		},
	}

	opts := dialect.MigrationOptions{IncludeUnsafe: true}
	result := &AlterTableResult{}
	g.generateColumnRemovals(td, "`users`", &opts, result)

	require.NotEmpty(t, result.Statements)
	assert.Contains(t, result.Statements[0], "DROP COLUMN")
}

func TestGenerateColumnRemovalsSafe(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		RemovedColumns: []*core.Column{
			{Name: "old_column", TypeRaw: "VARCHAR(100)", Nullable: true},
		},
	}

	opts := dialect.MigrationOptions{IncludeUnsafe: false}
	result := &AlterTableResult{}
	g.generateColumnRemovals(td, "`users`", &opts, result)

	require.NotEmpty(t, result.Statements)
	assert.Contains(t, result.Statements[0], "CHANGE COLUMN")
	assert.Contains(t, result.Statements[0], "__smf_backup_")
}

func TestGenerateColumnRemovalsNil(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name:           "users",
		RemovedColumns: []*core.Column{nil},
	}

	opts := dialect.MigrationOptions{IncludeUnsafe: true}
	result := &AlterTableResult{}
	g.generateColumnRemovals(td, "`users`", &opts, result)

	assert.Empty(t, result.Statements)
}

func TestGenerateOptionChanges(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		ModifiedOptions: []*diff.TableOptionChange{
			{Name: "ENGINE", Old: "MyISAM", New: "InnoDB"},
		},
	}

	result := &AlterTableResult{}
	g.generateOptionChanges(td, "`users`", result)

	require.NotEmpty(t, result.Statements)
	assert.Contains(t, result.Statements[0], "ENGINE=InnoDB")
}

func TestGenerateOptionChangesNil(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name:            "users",
		ModifiedOptions: []*diff.TableOptionChange{nil},
	}

	result := &AlterTableResult{}
	g.generateOptionChanges(td, "`users`", result)

	assert.Empty(t, result.Statements)
}

func TestGenerateOptionChangesEmptyNewValue(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		ModifiedOptions: []*diff.TableOptionChange{
			{Name: "ENGINE", Old: "InnoDB", New: ""},
		},
	}

	result := &AlterTableResult{}
	g.generateOptionChanges(td, "`users`", result)

	assert.Empty(t, result.Statements)
}

func TestGenerateConstraintAddsModifiedEmptyAddStmt(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "users",
		ModifiedConstraints: []*diff.ConstraintChange{
			{
				Old: &core.Constraint{Name: "chk_old", Type: core.ConstraintCheck, CheckExpression: "age > 0"},
				New: &core.Constraint{Name: "chk_new", Type: core.ConstraintCheck, CheckExpression: ""},
			},
		},
	}

	result := &AlterTableResult{}
	g.generateConstraintAdds(td, "`users`", result)

	assert.Empty(t, result.Statements)
	assert.Empty(t, result.FKStatements)
}

func TestGenerateConstraintAddsAddedEmptyAddStmt(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "orders",
		AddedConstraints: []*core.Constraint{
			{
				Name:              "fk_empty",
				Type:              core.ConstraintForeignKey,
				Columns:           []string{},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
			},
		},
	}

	result := &AlterTableResult{}
	g.generateConstraintAdds(td, "`orders`", result)

	assert.Empty(t, result.Statements)
	assert.Empty(t, result.FKStatements)
}

func TestGenerateConstraintAddsAddedEmptyReferencedTable(t *testing.T) {
	g := NewMySQLGenerator()

	td := &diff.TableDiff{
		Name: "orders",
		AddedConstraints: []*core.Constraint{
			{
				Name:              "fk_empty",
				Type:              core.ConstraintForeignKey,
				Columns:           []string{"user_id"},
				ReferencedTable:   "",
				ReferencedColumns: []string{"id"},
			},
		},
	}

	result := &AlterTableResult{}
	g.generateConstraintAdds(td, "`orders`", result)

	assert.Empty(t, result.Statements)
	assert.Empty(t, result.FKStatements)
}
