package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
)

func TestTimestampsDisabled(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:       "users",
				Columns:    []*core.Column{{Name: "id", Type: core.DataTypeInt}},
				Timestamps: &core.TimestampsConfig{Enabled: false},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}

func TestTimestampsDefaultDistinctNames(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:       "users",
				Columns:    []*core.Column{{Name: "id", Type: core.DataTypeInt}},
				Timestamps: &core.TimestampsConfig{Enabled: true},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}

func TestTimestampsSameNames(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:    "users",
				Columns: []*core.Column{{Name: "id", Type: core.DataTypeInt}},
				Timestamps: &core.TimestampsConfig{
					Enabled:       true,
					CreatedColumn: "created_at",
					UpdatedColumn: "created_at",
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolve to the same name")
}

func TestTimestampsCustomColumnValid(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:    "users",
				Columns: []*core.Column{{Name: "id", Type: core.DataTypeInt}},
				Timestamps: &core.TimestampsConfig{
					Enabled:       true,
					CreatedColumn: "creation_date",
					UpdatedColumn: "last_update",
				},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}

func TestTimestampsCustomColumnInvalidName(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name:    "users",
				Columns: []*core.Column{{Name: "id", Type: core.DataTypeInt}},
				Timestamps: &core.TimestampsConfig{
					Enabled:       true,
					CreatedColumn: "CreatedAt",
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be in snake_case")
}
