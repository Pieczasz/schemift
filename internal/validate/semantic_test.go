package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
)

func TestAutoIncrementOnNonInteger(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "id", Type: core.DataTypeString, AutoIncrement: true},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "auto_increment is only allowed on integer columns")
}

func TestAutoIncrementSQLiteOnNonPK(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectSQLite),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "id", Type: core.DataTypeInt, AutoIncrement: true},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SQLite AUTOINCREMENT is only allowed on PRIMARY KEY columns")
}

func TestAutoIncrementValid(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "id", Type: core.DataTypeInt, AutoIncrement: true, PrimaryKey: true},
				},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}

func TestNullablePKColumnLevel(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "id", Type: core.DataTypeInt, PrimaryKey: true, Nullable: true},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primary key columns cannot be nullable")
}

func TestNullablePKTableLevel(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "id", Type: core.DataTypeInt, Nullable: true},
				},
				Constraints: []*core.Constraint{
					{
						Type:    core.ConstraintPrimaryKey,
						Columns: []string{"id"},
					},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primary key columns cannot be nullable")
}

func TestGeneratedColumnWithoutExpression(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "id", Type: core.DataTypeInt, PrimaryKey: true},
					{Name: "full_name", Type: core.DataTypeString, IsGenerated: true},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "generated column must have an expression")
}

func TestIdentityOnNonAutoIncrement(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "id", Type: core.DataTypeInt, PrimaryKey: true, IdentitySeed: 100},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "identity_seed and identity_increment can only be set for auto_increment columns")
}

func TestTiDBAutoRandomOnNonPK(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectTiDB),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{
						Name: "id",
						Type: core.DataTypeInt,
						TiDB: &core.TiDBColumnOptions{ShardBits: 5},
					},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TiDB AUTO_RANDOM can only be applied to BIGINT PRIMARY KEY columns")
}

func TestTiDBAutoRandomOnNonInteger(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectTiDB),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{
						Name:       "id",
						Type:       core.DataTypeString,
						PrimaryKey: true,
						TiDB:       &core.TiDBColumnOptions{ShardBits: 5},
					},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TiDB AUTO_RANDOM can only be applied to BIGINT PRIMARY KEY columns")
}

func TestForeignKeyTypeMismatch(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "id", Type: core.DataTypeInt, PrimaryKey: true},
					{Name: "group_id", Type: core.DataTypeString, References: "groups.id"},
				},
			},
			{
				Name: "groups",
				Columns: []*core.Column{
					{Name: "id", Type: core.DataTypeInt, PrimaryKey: true},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "type mismatch between referencing column \"group_id\"")
}

func TestRawTypeInvalid(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{
						Name:    "id",
						Type:    core.DataTypeInt,
						RawType: "JSONB",
					},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not a valid type for dialect \"mysql\"")
}

func TestRawTypeValid(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{
						Name:    "id",
						Type:    core.DataTypeInt,
						RawType: "BIGINT",
					},
				},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}
