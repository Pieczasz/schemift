package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateDatabaseConstraintDuplicateNamesCaseInsensitive(t *testing.T) {
	d := DialectMySQL
	db := &Database{
		Name:    "app",
		Dialect: &d,
		Tables: []*Table{
			{
				Name:    "users",
				Columns: []*Column{{Name: "id"}},
				Constraints: []*Constraint{
					{Name: "uq_email", Type: ConstraintUnique, Columns: []string{"id"}},
					{Name: "UQ_EMAIL", Type: ConstraintUnique, Columns: []string{"id"}},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate constraint name")
}

func TestValidateDatabaseConstraintWithNoColumns(t *testing.T) {
	d := DialectMySQL
	db := &Database{
		Name:    "app",
		Dialect: &d,
		Tables: []*Table{
			{
				Name:    "users",
				Columns: []*Column{{Name: "id"}},
				Constraints: []*Constraint{
					{Name: "uq_users_id", Type: ConstraintUnique},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has no columns")
}

func TestValidateDatabaseConstraintReferencesNonexistentColumn(t *testing.T) {
	d := DialectMySQL
	db := &Database{
		Name:    "app",
		Dialect: &d,
		Tables: []*Table{
			{
				Name:    "users",
				Columns: []*Column{{Name: "id"}},
				Constraints: []*Constraint{
					{Name: "uq_users_email", Type: ConstraintUnique, Columns: []string{"email"}},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "references nonexistent column")
}

func TestValidateDatabaseConstraintForeignKeyMissingReferencedTable(t *testing.T) {
	d := DialectMySQL
	db := &Database{
		Name:    "app",
		Dialect: &d,
		Tables: []*Table{
			{
				Name:    "users",
				Columns: []*Column{{Name: "role_id"}},
				Constraints: []*Constraint{
					{Name: "fk_users_role", Type: ConstraintForeignKey, Columns: []string{"role_id"}},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing referenced_table")
}

func TestValidateDatabaseConstraintForeignKeyMissingReferencedColumns(t *testing.T) {
	d := DialectMySQL
	db := &Database{
		Name:    "app",
		Dialect: &d,
		Tables: []*Table{
			{
				Name:    "users",
				Columns: []*Column{{Name: "role_id"}},
				Constraints: []*Constraint{
					{
						Name:            "fk_users_role",
						Type:            ConstraintForeignKey,
						Columns:         []string{"role_id"},
						ReferencedTable: "roles",
					},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing referenced_columns")
}

func TestValidateDatabaseCheckConstraintMayHaveNoColumns(t *testing.T) {
	d := DialectMySQL
	db := &Database{
		Name:    "app",
		Dialect: &d,
		Tables: []*Table{
			{
				Name:    "users",
				Columns: []*Column{{Name: "age"}},
				Constraints: []*Constraint{
					{Name: "chk_age", Type: ConstraintCheck, CheckExpression: "age >= 0"},
				},
			},
		},
	}

	err := ValidateDatabase(db)
	require.NoError(t, err)
}
