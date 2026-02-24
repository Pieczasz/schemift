package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
)

func TestValidateEnumsColumnType(t *testing.T) {
	tests := []struct {
		name    string
		db      *core.Database
		wantErr string
	}{
		{
			name: "invalid column type",
			db: &core.Database{
				Name:    "app",
				Dialect: new(core.DialectMySQL),
				Tables: []*core.Table{
					{
						Name: "users",
						Columns: []*core.Column{
							{Name: "id", Type: "BANANA"},
						},
					},
				},
			},
			wantErr: "invalid type \"BANANA\"",
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

func TestValidateEnumsColumnRefActions(t *testing.T) {
	tests := []struct {
		name    string
		db      *core.Database
		wantErr string
	}{
		{
			name: "invalid ref_on_delete",
			db: &core.Database{
				Name:    "app",
				Dialect: new(core.DialectMySQL),
				Tables: []*core.Table{
					{
						Name: "users",
						Columns: []*core.Column{
							{Name: "id", Type: core.DataTypeInt, PrimaryKey: true},
							{Name: "role_id", Type: core.DataTypeInt, References: "roles.id", RefOnDelete: "OOPS"},
						},
					},
					{
						Name: "roles",
						Columns: []*core.Column{
							{Name: "id", Type: core.DataTypeInt, PrimaryKey: true},
						},
					},
				},
			},
			wantErr: "invalid ref_on_delete \"OOPS\"",
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

func TestEnumsColumnType(t *testing.T) {
	tests := []struct {
		name    string
		col     *core.Column
		table   *core.Table
		wantErr string
	}{
		{
			name:    "invalid type",
			col:     &core.Column{Name: "id", Type: "BANANA"},
			table:   &core.Table{Name: "users"},
			wantErr: "invalid type",
		},
		{
			name:    "valid string type",
			col:     &core.Column{Name: "name", Type: core.DataTypeString},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid int type",
			col:     &core.Column{Name: "id", Type: core.DataTypeInt},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid bool type",
			col:     &core.Column{Name: "active", Type: core.DataTypeBoolean},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid datetime type",
			col:     &core.Column{Name: "created_at", Type: core.DataTypeDatetime},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid json type",
			col:     &core.Column{Name: "data", Type: core.DataTypeJSON},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid uuid type",
			col:     &core.Column{Name: "uuid", Type: core.DataTypeUUID},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid binary type",
			col:     &core.Column{Name: "data", Type: core.DataTypeBinary},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid enum type",
			col:     &core.Column{Name: "status", Type: core.DataTypeEnum},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "empty type is valid (handled elsewhere)",
			col:     &core.Column{Name: "id"},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ColumnType(tt.col, tt.table)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestEnumsRefActions(t *testing.T) {
	tests := []struct {
		name    string
		col     *core.Column
		table   *core.Table
		wantErr string
	}{
		{
			name:    "invalid ref_on_delete",
			col:     &core.Column{Name: "id", RefOnDelete: "INVALID"},
			table:   &core.Table{Name: "users"},
			wantErr: "invalid ref_on_delete",
		},
		{
			name:    "invalid ref_on_update",
			col:     &core.Column{Name: "id", RefOnUpdate: "INVALID"},
			table:   &core.Table{Name: "users"},
			wantErr: "invalid ref_on_update",
		},
		{
			name:    "valid ref actions",
			col:     &core.Column{Name: "id", RefOnDelete: core.RefActionCascade, RefOnUpdate: core.RefActionSetNull},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "empty ref actions valid",
			col:     &core.Column{Name: "id"},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := RefActions(tt.col, tt.table)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestEnumsGeneration(t *testing.T) {
	tests := []struct {
		name    string
		col     *core.Column
		table   *core.Table
		wantErr string
	}{
		{
			name:    "invalid generation_storage",
			col:     &core.Column{Name: "id", IsGenerated: true, GenerationStorage: "INVALID"},
			table:   &core.Table{Name: "users"},
			wantErr: "invalid generation_storage",
		},
		{
			name:    "valid virtual",
			col:     &core.Column{Name: "id", IsGenerated: true, GenerationStorage: core.GenerationVirtual},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid stored",
			col:     &core.Column{Name: "id", IsGenerated: true, GenerationStorage: core.GenerationStored},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "not generated - no error",
			col:     &core.Column{Name: "id", GenerationStorage: "SOMEVALUE"},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Generation(tt.col, tt.table)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestEnumsIdentity(t *testing.T) {
	tests := []struct {
		name    string
		col     *core.Column
		table   *core.Table
		wantErr string
	}{
		{
			name:    "invalid identity_generation",
			col:     &core.Column{Name: "id", IdentityGeneration: "INVALID"},
			table:   &core.Table{Name: "users"},
			wantErr: "invalid identity_generation",
		},
		{
			name:    "valid always",
			col:     &core.Column{Name: "id", IdentityGeneration: core.IdentityAlways},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid by default",
			col:     &core.Column{Name: "id", IdentityGeneration: core.IdentityByDefault},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "empty identity valid",
			col:     &core.Column{Name: "id"},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Identity(tt.col, tt.table)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConstraintEnumsInvalid(t *testing.T) {
	tests := []struct {
		name    string
		con     *core.Constraint
		table   *core.Table
		wantErr string
	}{
		{
			name:    "invalid constraint type",
			con:     &core.Constraint{Name: "invalid", Type: "INVALID"},
			table:   &core.Table{Name: "users"},
			wantErr: "invalid constraint type",
		},
		{
			name:    "invalid on_delete",
			con:     &core.Constraint{Name: "fk", Type: core.ConstraintForeignKey, OnDelete: "INVALID"},
			table:   &core.Table{Name: "users"},
			wantErr: "invalid on_delete",
		},
		{
			name:    "invalid on_update",
			con:     &core.Constraint{Name: "fk", Type: core.ConstraintForeignKey, OnUpdate: "INVALID"},
			table:   &core.Table{Name: "users"},
			wantErr: "invalid on_update",
		},
		{
			name:    "valid primary key",
			con:     &core.Constraint{Name: "pk", Type: core.ConstraintPrimaryKey},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid unique",
			con:     &core.Constraint{Name: "uq", Type: core.ConstraintUnique},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid check",
			con:     &core.Constraint{Name: "chk", Type: core.ConstraintCheck},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
		{
			name:    "valid FK with actions",
			con:     &core.Constraint{Name: "fk", Type: core.ConstraintForeignKey, OnDelete: core.RefActionCascade, OnUpdate: core.RefActionRestrict},
			table:   &core.Table{Name: "users"},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ConstraintEnums(tt.con, tt.table)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestIndexEnumsInvalidType(t *testing.T) {
	err := IndexEnums(&core.Index{Name: "idx", Type: "INVALID"}, &core.Table{Name: "users"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid index type")
}

func TestIndexEnumsInvalidVisibility(t *testing.T) {
	err := IndexEnums(&core.Index{Name: "idx", Visibility: "INVALID"}, &core.Table{Name: "users"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid visibility")
}

func TestIndexEnumsInvalidSortOrder(t *testing.T) {
	err := IndexEnums(&core.Index{Name: "idx", Columns: []core.ColumnIndex{{Name: "id", Order: "INVALID"}}}, &core.Table{Name: "users"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid sort order")
}

func TestIndexEnumsValidTypes(t *testing.T) {
	tests := []*core.Index{
		{Name: "idx", Type: core.IndexTypeBTree},
		{Name: "idx", Type: core.IndexTypeHash},
		{Name: "idx", Type: core.IndexTypeFullText},
		{Name: "idx", Type: core.IndexTypeSpatial},
		{Name: "idx", Type: core.IndexTypeGIN},
		{Name: "idx", Type: core.IndexTypeGiST},
		{Name: "idx", Type: ""},
	}

	for _, idx := range tests {
		err := IndexEnums(idx, &core.Table{Name: "users"})
		assert.NoError(t, err)
	}
}

func TestIndexEnumsValidVisibility(t *testing.T) {
	tests := []*core.Index{
		{Name: "idx", Visibility: core.IndexVisible},
		{Name: "idx", Visibility: core.IndexInvisible},
		{Name: "idx", Visibility: ""},
	}

	for _, idx := range tests {
		err := IndexEnums(idx, &core.Table{Name: "users"})
		assert.NoError(t, err)
	}
}

func TestIndexEnumsValidOrder(t *testing.T) {
	tests := []struct {
		idx *core.Index
	}{
		{idx: &core.Index{Name: "idx", Columns: []core.ColumnIndex{{Name: "id", Order: core.SortAsc}}}},
		{idx: &core.Index{Name: "idx", Columns: []core.ColumnIndex{{Name: "id", Order: core.SortDesc}}}},
		{idx: &core.Index{Name: "idx", Columns: []core.ColumnIndex{{Name: "id", Order: ""}}}},
	}

	for _, tt := range tests {
		err := IndexEnums(tt.idx, &core.Table{Name: "users"})
		assert.NoError(t, err)
	}
}
