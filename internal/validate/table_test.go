package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
)

func TestTableNoColumns(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{Name: "users"},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table \"users\" has no columns")
}

func TestTableDuplicateColumnNames(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "email"},
					{Name: "email"},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate column name")
}

func TestTableEmptyColumnName(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "   "},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name is empty")
}

func TestTableMaxColumnNameLength(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Validation: &core.ValidationRules{
			MaxColumnNameLength: 3,
		},
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "email"},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `exceeds maximum length 3`)
}

func TestTableAllowedNamePatternForColumn(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Validation: &core.ValidationRules{
			AllowedNamePattern: `^u[a-z]+$`,
		},
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "email"},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match allowed pattern")
}

func TestPrimaryKeyConflictMultipleConstraints(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "id"},
				},
				Constraints: []*core.Constraint{
					{Name: "pk1", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}},
					{Name: "pk2", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multiple PRIMARY KEY constraints")
}

func TestPrimaryKeyConflictColumnAndConstraint(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "id", PrimaryKey: true},
				},
				Constraints: []*core.Constraint{
					{Name: "pk_users", Type: core.ConstraintPrimaryKey, Columns: []string{"id"}},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primary key declared on both")
}

func TestSynthesizeConstraints(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "id", Type: core.DataTypeInt, PrimaryKey: true},
					{Name: "email", Type: core.DataTypeString, Unique: true},
					{Name: "age", Type: core.DataTypeInt, Check: "age >= 0"},
					{Name: "role_id", Type: core.DataTypeInt, References: "roles.id", RefOnDelete: core.RefActionCascade, RefOnUpdate: core.RefActionRestrict},
				},
			},
			{
				Name: "roles",
				Columns: []*core.Column{
					{Name: "id", Type: core.DataTypeInt, PrimaryKey: true},
				},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)

	users := db.Tables[0]

	var uniqueCount, checkCount, fkCount int
	for _, c := range users.Constraints {
		switch c.Type {
		case core.ConstraintUnique:
			uniqueCount++
		case core.ConstraintCheck:
			checkCount++
		case core.ConstraintForeignKey:
			fkCount++
			assert.Equal(t, "roles", c.ReferencedTable)
			assert.Equal(t, []string{"id"}, c.ReferencedColumns)
			assert.Equal(t, core.RefActionCascade, c.OnDelete)
			assert.Equal(t, core.RefActionRestrict, c.OnUpdate)
		}
	}
	assert.Equal(t, 1, uniqueCount)
	assert.Equal(t, 1, checkCount)
	assert.Equal(t, 1, fkCount)
}
