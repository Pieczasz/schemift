package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidationErrorError(t *testing.T) {
	t.Run("error with field", func(t *testing.T) {
		err := &ValidationError{
			Entity:  "column",
			Name:    "email",
			Field:   "TypeRaw",
			Message: "column type is empty",
		}
		expected := `validation error in column "email" field "TypeRaw": column type is empty`
		assert.Equal(t, expected, err.Error())
	})

	t.Run("error without field", func(t *testing.T) {
		err := &ValidationError{
			Entity:  "table",
			Name:    "users",
			Message: "table has no columns",
		}
		expected := `validation error in table "users": table has no columns`
		assert.Equal(t, expected, err.Error())
	})

	t.Run("error with empty name", func(t *testing.T) {
		err := &ValidationError{
			Entity:  "database",
			Name:    "",
			Message: "database is nil",
		}
		expected := `validation error in database "": database is nil`
		assert.Equal(t, expected, err.Error())
	})
}

func TestDatabaseValidate(t *testing.T) {
	t.Run("nil database", func(t *testing.T) {
		var db *Database
		err := db.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "database is nil")
	})

	t.Run("valid database", func(t *testing.T) {
		db := &Database{
			Name: "testdb",
			Tables: []*Table{
				{
					Name: "users",
					Columns: []*Column{
						{Name: "id", TypeRaw: "INT"},
					},
				},
			},
		}
		err := db.Validate()
		assert.NoError(t, err)
	})

	t.Run("nil table in database", func(t *testing.T) {
		db := &Database{
			Name: "testdb",
			Tables: []*Table{
				{
					Name: "users",
					Columns: []*Column{
						{Name: "id", TypeRaw: "INT"},
					},
				},
				nil,
			},
		}
		err := db.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "table at index 1 is nil")
	})

	t.Run("duplicate table names", func(t *testing.T) {
		db := &Database{
			Name: "testdb",
			Tables: []*Table{
				{
					Name: "users",
					Columns: []*Column{
						{Name: "id", TypeRaw: "INT"},
					},
				},
				{
					Name: "Users",
					Columns: []*Column{
						{Name: "id", TypeRaw: "INT"},
					},
				},
			},
		}
		err := db.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate table name")
	})

	t.Run("invalid table in database", func(t *testing.T) {
		db := &Database{
			Name: "testdb",
			Tables: []*Table{
				{
					Name:    "users",
					Columns: []*Column{},
				},
			},
		}
		err := db.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "table has no columns")
	})

	t.Run("empty database with no tables", func(t *testing.T) {
		db := &Database{
			Name:   "testdb",
			Tables: []*Table{},
		}
		err := db.Validate()
		assert.NoError(t, err)
	})
}

func TestTableValidate(t *testing.T) {
	t.Run("nil table", func(t *testing.T) {
		var table *Table
		err := table.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "table is nil")
	})

	t.Run("empty table name", func(t *testing.T) {
		table := &Table{
			Name: "",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
			},
		}
		err := table.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "table name is empty")
	})

	t.Run("whitespace only table name", func(t *testing.T) {
		table := &Table{
			Name: "   ",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
			},
		}
		err := table.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "table name is empty")
	})

	t.Run("table with no columns", func(t *testing.T) {
		table := &Table{
			Name:    "users",
			Columns: []*Column{},
		}
		err := table.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "table has no columns")
	})

	t.Run("nil column in table", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
				nil,
			},
		}
		err := table.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column at index 1 is nil")
	})

	t.Run("invalid column in table", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
				{Name: "", TypeRaw: "VARCHAR(255)"},
			},
		}
		err := table.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column name is empty")
	})

	t.Run("duplicate column names", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
				{Name: "ID", TypeRaw: "INT"},
			},
		}
		err := table.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate column name")
	})

	t.Run("nil constraint in table", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
			},
			Constraints: []*Constraint{
				{Name: "pk_users", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
				nil,
			},
		}
		err := table.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "constraint at index 1 is nil")
	})

	t.Run("invalid constraint in table", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
			},
			Constraints: []*Constraint{
				{Name: "pk_users", Type: ConstraintPrimaryKey, Columns: []string{}},
			},
		}
		err := table.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "constraint has no columns")
	})

	t.Run("duplicate constraint names", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
				{Name: "email", TypeRaw: "VARCHAR(255)"},
			},
			Constraints: []*Constraint{
				{Name: "pk_users", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
				{Name: "PK_Users", Type: ConstraintUnique, Columns: []string{"email"}},
			},
		}
		err := table.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate constraint name")
	})

	t.Run("empty constraint names are allowed", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
				{Name: "email", TypeRaw: "VARCHAR(255)"},
			},
			Constraints: []*Constraint{
				{Name: "", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
				{Name: "", Type: ConstraintUnique, Columns: []string{"email"}},
			},
		}
		err := table.Validate()
		assert.NoError(t, err)
	})

	t.Run("nil index in table", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
			},
			Indexes: []*Index{
				{Name: "idx_id", Columns: []IndexColumn{{Name: "id"}}},
				nil,
			},
		}
		err := table.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "index at index 1 is nil")
	})

	t.Run("invalid index in table", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
			},
			Indexes: []*Index{
				{Name: "idx_id", Columns: []IndexColumn{}},
			},
		}
		err := table.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "index has no columns")
	})

	t.Run("duplicate index names", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
				{Name: "email", TypeRaw: "VARCHAR(255)"},
			},
			Indexes: []*Index{
				{Name: "idx_email", Columns: []IndexColumn{{Name: "email"}}},
				{Name: "IDX_Email", Columns: []IndexColumn{{Name: "id"}}},
			},
		}
		err := table.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate index name")
	})

	t.Run("empty index names are allowed", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
				{Name: "email", TypeRaw: "VARCHAR(255)"},
			},
			Indexes: []*Index{
				{Name: "", Columns: []IndexColumn{{Name: "id"}}},
				{Name: "", Columns: []IndexColumn{{Name: "email"}}},
			},
		}
		err := table.Validate()
		assert.NoError(t, err)
	})

	t.Run("valid table with all components", func(t *testing.T) {
		table := &Table{
			Name: "users",
			Columns: []*Column{
				{Name: "id", TypeRaw: "INT"},
				{Name: "email", TypeRaw: "VARCHAR(255)"},
			},
			Constraints: []*Constraint{
				{Name: "pk_users", Type: ConstraintPrimaryKey, Columns: []string{"id"}},
			},
			Indexes: []*Index{
				{Name: "idx_email", Columns: []IndexColumn{{Name: "email"}}},
			},
		}
		err := table.Validate()
		assert.NoError(t, err)
	})
}

func TestColumnValidate(t *testing.T) {
	t.Run("nil column", func(t *testing.T) {
		var col *Column
		err := col.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column is nil")
	})

	t.Run("empty column name", func(t *testing.T) {
		col := &Column{Name: "", TypeRaw: "INT"}
		err := col.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column name is empty")
	})

	t.Run("whitespace only column name", func(t *testing.T) {
		col := &Column{Name: "   ", TypeRaw: "INT"}
		err := col.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column name is empty")
	})

	t.Run("empty column type", func(t *testing.T) {
		col := &Column{Name: "id", TypeRaw: ""}
		err := col.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column type is empty")
	})

	t.Run("whitespace only column type", func(t *testing.T) {
		col := &Column{Name: "id", TypeRaw: "   "}
		err := col.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "column type is empty")
	})

	t.Run("generated column without expression", func(t *testing.T) {
		col := &Column{
			Name:                 "full_name",
			TypeRaw:              "VARCHAR(255)",
			IsGenerated:          true,
			GenerationExpression: "",
		}
		err := col.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "generated column must have an expression")
	})

	t.Run("generated column with whitespace only expression", func(t *testing.T) {
		col := &Column{
			Name:                 "full_name",
			TypeRaw:              "VARCHAR(255)",
			IsGenerated:          true,
			GenerationExpression: "   ",
		}
		err := col.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "generated column must have an expression")
	})

	t.Run("valid generated column", func(t *testing.T) {
		col := &Column{
			Name:                 "full_name",
			TypeRaw:              "VARCHAR(255)",
			IsGenerated:          true,
			GenerationExpression: "CONCAT(first_name, ' ', last_name)",
			GenerationStorage:    GenerationVirtual,
		}
		err := col.Validate()
		assert.NoError(t, err)
	})

	t.Run("valid regular column", func(t *testing.T) {
		col := &Column{
			Name:    "email",
			TypeRaw: "VARCHAR(255)",
		}
		err := col.Validate()
		assert.NoError(t, err)
	})
}

func TestConstraintValidate(t *testing.T) {
	t.Run("nil constraint", func(t *testing.T) {
		var c *Constraint
		err := c.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "constraint is nil")
	})

	t.Run("primary key without columns", func(t *testing.T) {
		c := &Constraint{
			Name:    "pk_users",
			Type:    ConstraintPrimaryKey,
			Columns: []string{},
		}
		err := c.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "constraint has no columns")
	})

	t.Run("unique constraint without columns", func(t *testing.T) {
		c := &Constraint{
			Name:    "uq_email",
			Type:    ConstraintUnique,
			Columns: []string{},
		}
		err := c.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "constraint has no columns")
	})

	t.Run("foreign key without referenced table", func(t *testing.T) {
		c := &Constraint{
			Name:              "fk_order_user",
			Type:              ConstraintForeignKey,
			Columns:           []string{"user_id"},
			ReferencedTable:   "",
			ReferencedColumns: []string{"id"},
		}
		err := c.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "foreign key must reference a table")
	})

	t.Run("foreign key with whitespace only referenced table", func(t *testing.T) {
		c := &Constraint{
			Name:              "fk_order_user",
			Type:              ConstraintForeignKey,
			Columns:           []string{"user_id"},
			ReferencedTable:   "   ",
			ReferencedColumns: []string{"id"},
		}
		err := c.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "foreign key must reference a table")
	})

	t.Run("foreign key without referenced columns", func(t *testing.T) {
		c := &Constraint{
			Name:              "fk_order_user",
			Type:              ConstraintForeignKey,
			Columns:           []string{"user_id"},
			ReferencedTable:   "users",
			ReferencedColumns: []string{},
		}
		err := c.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "foreign key must reference columns")
	})

	t.Run("foreign key column count mismatch", func(t *testing.T) {
		c := &Constraint{
			Name:              "fk_order_user",
			Type:              ConstraintForeignKey,
			Columns:           []string{"user_id", "org_id"},
			ReferencedTable:   "users",
			ReferencedColumns: []string{"id"},
		}
		err := c.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "foreign key column count mismatch")
	})

	t.Run("check constraint without expression", func(t *testing.T) {
		c := &Constraint{
			Name:            "chk_age",
			Type:            ConstraintCheck,
			CheckExpression: "",
		}
		err := c.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "check constraint must have an expression")
	})

	t.Run("check constraint with whitespace only expression", func(t *testing.T) {
		c := &Constraint{
			Name:            "chk_age",
			Type:            ConstraintCheck,
			CheckExpression: "   ",
		}
		err := c.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "check constraint must have an expression")
	})

	t.Run("valid primary key constraint", func(t *testing.T) {
		c := &Constraint{
			Name:    "pk_users",
			Type:    ConstraintPrimaryKey,
			Columns: []string{"id"},
		}
		err := c.Validate()
		assert.NoError(t, err)
	})

	t.Run("valid unique constraint", func(t *testing.T) {
		c := &Constraint{
			Name:    "uq_email",
			Type:    ConstraintUnique,
			Columns: []string{"email"},
		}
		err := c.Validate()
		assert.NoError(t, err)
	})

	t.Run("valid foreign key constraint", func(t *testing.T) {
		c := &Constraint{
			Name:              "fk_order_user",
			Type:              ConstraintForeignKey,
			Columns:           []string{"user_id"},
			ReferencedTable:   "users",
			ReferencedColumns: []string{"id"},
			OnDelete:          RefActionCascade,
			OnUpdate:          RefActionRestrict,
		}
		err := c.Validate()
		assert.NoError(t, err)
	})

	t.Run("valid check constraint", func(t *testing.T) {
		c := &Constraint{
			Name:            "chk_age",
			Type:            ConstraintCheck,
			CheckExpression: "age >= 0 AND age <= 150",
			Enforced:        true,
		}
		err := c.Validate()
		assert.NoError(t, err)
	})

	t.Run("check constraint can have empty columns", func(t *testing.T) {
		c := &Constraint{
			Name:            "chk_age",
			Type:            ConstraintCheck,
			Columns:         []string{},
			CheckExpression: "age >= 0",
		}
		err := c.Validate()
		assert.NoError(t, err)
	})
}

func TestIndexValidate(t *testing.T) {
	t.Run("nil index", func(t *testing.T) {
		var idx *Index
		err := idx.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "index is nil")
	})

	t.Run("index without columns", func(t *testing.T) {
		idx := &Index{
			Name:    "idx_email",
			Columns: []IndexColumn{},
		}
		err := idx.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "index has no columns")
	})

	t.Run("index with empty column name", func(t *testing.T) {
		idx := &Index{
			Name: "idx_composite",
			Columns: []IndexColumn{
				{Name: "email"},
				{Name: ""},
			},
		}
		err := idx.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "index column at position 1 has empty name")
	})

	t.Run("index with whitespace only column name", func(t *testing.T) {
		idx := &Index{
			Name: "idx_composite",
			Columns: []IndexColumn{
				{Name: "   "},
			},
		}
		err := idx.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "index column at position 0 has empty name")
	})

	t.Run("valid index with single column", func(t *testing.T) {
		idx := &Index{
			Name: "idx_email",
			Columns: []IndexColumn{
				{Name: "email"},
			},
		}
		err := idx.Validate()
		assert.NoError(t, err)
	})

	t.Run("valid index with multiple columns", func(t *testing.T) {
		idx := &Index{
			Name: "idx_composite",
			Columns: []IndexColumn{
				{Name: "first_name", Order: SortAsc},
				{Name: "last_name", Order: SortDesc},
			},
			Unique: true,
			Type:   IndexTypeBTree,
		}
		err := idx.Validate()
		assert.NoError(t, err)
	})

	t.Run("valid index with length", func(t *testing.T) {
		idx := &Index{
			Name: "idx_content",
			Columns: []IndexColumn{
				{Name: "content", Length: 100},
			},
		}
		err := idx.Validate()
		assert.NoError(t, err)
	})

	t.Run("valid index without name", func(t *testing.T) {
		idx := &Index{
			Columns: []IndexColumn{
				{Name: "email"},
			},
		}
		err := idx.Validate()
		assert.NoError(t, err)
	})
}
