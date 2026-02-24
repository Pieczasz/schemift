package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
)

func TestColumnInvalidReferencesFormat(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "role_id", Type: core.DataTypeInt, References: "roles"},
				},
			},
		},
	}

	err := Database(db)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `invalid references "roles"`)
}

func TestColumnEmptyType(t *testing.T) {
	tests := []struct {
		name string
		col  *core.Column
	}{
		{
			name: "empty type and rawtype",
			col:  &core.Column{Name: "id"},
		},
		{
			name: "unknown type",
			col:  &core.Column{Name: "id", Type: core.DataTypeUnknown},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &core.Database{
				Name:    "app",
				Dialect: new(core.DialectMySQL),
				Tables: []*core.Table{
					{
						Name:    "users",
						Columns: []*core.Column{tt.col},
					},
				},
			}

			err := Database(db)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "type is empty")
		})
	}
}

func TestColumnValid(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "id", Type: core.DataTypeInt},
				},
			},
		},
	}

	err := Database(db)
	require.NoError(t, err)
}

func TestColumnValidReferences(t *testing.T) {
	db := &core.Database{
		Name:    "app",
		Dialect: new(core.DialectMySQL),
		Tables: []*core.Table{
			{
				Name: "users",
				Columns: []*core.Column{
					{Name: "id", Type: core.DataTypeInt, PrimaryKey: true},
					{Name: "role_id", Type: core.DataTypeInt, References: "roles.id"},
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
}
