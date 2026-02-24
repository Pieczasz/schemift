package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
)

func TestConstraintDuplicateNames(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:    "users",
				Columns: []*core.Column{{Name: "id", Type: core.DataTypeInt}},
				Constraints: []*core.Constraint{
					{Name: "uq_email", Type: core.ConstraintUnique, Columns: []string{"id"}},
					{Name: "uq_email", Type: core.ConstraintUnique, Columns: []string{"id"}},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate constraint name")
}

func TestConstraintWithNoColumns(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:    "users",
				Columns: []*core.Column{{Name: "id", Type: core.DataTypeInt}},
				Constraints: []*core.Constraint{
					{Name: "uq_users_id", Type: core.ConstraintUnique, Columns: []string{}},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has no columns")
}

func TestConstraintReferencesNonexistentColumn(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:    "users",
				Columns: []*core.Column{{Name: "id", Type: core.DataTypeInt}},
				Constraints: []*core.Constraint{
					{Name: "uq_users_email", Type: core.ConstraintUnique, Columns: []string{"email"}},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "references nonexistent column")
}

func TestConstraintForeignKeyMissingReferencedTable(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:    "users",
				Columns: []*core.Column{{Name: "role_id", Type: core.DataTypeInt}},
				Constraints: []*core.Constraint{
					{Name: "fk_users_role", Type: core.ConstraintForeignKey, Columns: []string{"role_id"}},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing referenced_table")
}

func TestConstraintForeignKeyMissingReferencedColumns(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:    "users",
				Columns: []*core.Column{{Name: "role_id", Type: core.DataTypeInt}},
				Constraints: []*core.Constraint{
					{
						Name:            "fk_users_role",
						Type:            core.ConstraintForeignKey,
						Columns:         []string{"role_id"},
						ReferencedTable: "roles",
					},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing referenced_columns")
}

func TestConstraintCheckMayHaveNoColumns(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:    "users",
				Columns: []*core.Column{{Name: "age", Type: core.DataTypeInt}},
				Constraints: []*core.Constraint{
					{Name: "chk_age", Type: core.ConstraintCheck, CheckExpression: "age >= 0"},
				},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}

func TestForeignKeyValidReference(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:    "users",
				Columns: []*core.Column{{Name: "id", Type: core.DataTypeInt}},
			},
			{
				Name:    "posts",
				Columns: []*core.Column{{Name: "author_id", Type: core.DataTypeInt}},
				Constraints: []*core.Constraint{
					{
						Name:              "fk_posts_author",
						Type:              core.ConstraintForeignKey,
						Columns:           []string{"author_id"},
						ReferencedTable:   "users",
						ReferencedColumns: []string{"id"},
					},
				},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}

func TestForeignKeyNonExistentTable(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:    "posts",
				Columns: []*core.Column{{Name: "author_id", Type: core.DataTypeInt}},
				Constraints: []*core.Constraint{
					{
						Name:              "fk_posts_author",
						Type:              core.ConstraintForeignKey,
						Columns:           []string{"author_id"},
						ReferencedTable:   "users",
						ReferencedColumns: []string{"id"},
					},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `references non-existent table "users"`)
}

func TestForeignKeyNonExistentColumn(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:    "users",
				Columns: []*core.Column{{Name: "id", Type: core.DataTypeInt}},
			},
			{
				Name:    "posts",
				Columns: []*core.Column{{Name: "author_id", Type: core.DataTypeInt}},
				Constraints: []*core.Constraint{
					{
						Name:              "fk_posts_author",
						Type:              core.ConstraintForeignKey,
						Columns:           []string{"author_id"},
						ReferencedTable:   "users",
						ReferencedColumns: []string{"uuid"},
					},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `references non-existent column "uuid" in table "users"`)
}
