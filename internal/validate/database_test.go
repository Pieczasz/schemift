package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
)

func TestDatabaseRequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		db      *core.Database
		wantErr string
	}{
		{
			name:    "nil database",
			db:      nil,
			wantErr: "database is nil",
		},
		{
			name: "missing dialect",
			db: &core.Database{
				Name: "app",
			},
			wantErr: "dialect is required",
		},
		{
			name: "invalid dialect",
			db: &core.Database{
				Name:    "app",
				Dialect: new(core.Dialect),
			},
			wantErr: "unsupported dialect",
		},
		{
			name: "missing database name",
			db: &core.Database{
				Name:    "",
				Dialect: new(core.DialectMySQL),
			},
			wantErr: "database name is required",
		},
		{
			name: "empty tables",
			db: &core.Database{
				Name:    "app",
				Dialect: new(core.DialectMySQL),
				Tables:  []*core.Table{},
			},
			wantErr: "schema is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Database(tt.db)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestDatabaseValid(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "id", Type: core.DataTypeInt, PrimaryKey: true},
				},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}

func TestDatabaseDuplicateTableNames(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{Name: "users", Columns: []*core.Column{{Name: "id", Type: core.DataTypeInt}}},
			{Name: "users", Columns: []*core.Column{{Name: "id", Type: core.DataTypeInt}}},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate table name")
}

func TestDatabaseInvalidAllowedNamePattern(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Validation: &core.ValidationRules{
			AllowedNamePattern: "(",
		},
		Tables: []*core.Table{
			{Name: "users", Columns: []*core.Column{{Name: "id", Type: core.DataTypeInt}}},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid allowed_name_pattern")
}

func TestTableName(t *testing.T) {
	tests := []struct {
		name    string
		db      *core.Database
		wantErr string
	}{
		{
			name: "invalid table name - not snake_case",
			db: &core.Database{
				Name:    "app",
				Dialect: new(core.DialectMySQL),
				Tables: []*core.Table{
					{Name: "Users", Columns: []*core.Column{{Name: "id", Type: core.DataTypeInt}}},
				},
			},
			wantErr: "must be in snake_case",
		},
		{
			name: "table name exceeds max length",
			db: &core.Database{
				Name:    "app",
				Dialect: new(core.DialectMySQL),
				Validation: &core.ValidationRules{
					MaxTableNameLength: 3,
				},
				Tables: []*core.Table{
					{Name: "users", Columns: []*core.Column{{Name: "id", Type: core.DataTypeInt}}},
				},
			},
			wantErr: "exceeds maximum length",
		},
		{
			name: "table name does not match allowed pattern",
			db: &core.Database{
				Name:    "app",
				Dialect: new(core.DialectMySQL),
				Validation: &core.ValidationRules{
					AllowedNamePattern: "^u[a-z]+$",
				},
				Tables: []*core.Table{
					{Name: "users", Columns: []*core.Column{{Name: "id", Type: core.DataTypeInt}}},
				},
			},
			wantErr: "does not match allowed pattern",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Database(tt.db)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
