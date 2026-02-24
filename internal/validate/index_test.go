package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
)

func TestIndexDuplicateNames(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:    "users",
				Columns: []*core.Column{{Name: "email", Type: core.DataTypeString}},
				Indexes: []*core.Index{
					{Name: "idx_email", Columns: []core.ColumnIndex{{Name: "email"}}},
					{Name: "idx_email", Columns: []core.ColumnIndex{{Name: "email"}}},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate index name")
}

func TestIndexHasNoColumns(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:    "users",
				Columns: []*core.Column{{Name: "email", Type: core.DataTypeString}},
				Indexes: []*core.Index{
					{Name: "idx_email"},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "index idx_email has no columns")
}

func TestIndexUnnamedHasNoColumns(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:    "users",
				Columns: []*core.Column{{Name: "email", Type: core.DataTypeString}},
				Indexes: []*core.Index{
					{},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "index (unnamed) has no columns")
}

func TestIndexReferencesNonexistentColumn(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:    "users",
				Columns: []*core.Column{{Name: "email", Type: core.DataTypeString}},
				Indexes: []*core.Index{
					{Name: "idx_missing", Columns: []core.ColumnIndex{{Name: "missing"}}},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `index "idx_missing" references nonexistent column "missing"`)
}

func TestIndexValid(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:    "users",
				Columns: []*core.Column{{Name: "email", Type: core.DataTypeString}},
				Indexes: []*core.Index{
					{Name: "idx_email", Columns: []core.ColumnIndex{{Name: "email"}}},
				},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}
