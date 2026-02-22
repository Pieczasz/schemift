package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
)

func TestIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"empty", "", false},
		{"valid simple", "users", true},
		{"valid with number", "user123", true},
		{"valid with underscore", "user_name", true},
		{"valid starting underscore", "_user", true},
		{"invalid starting number", "123user", false},
		{"invalid with dash", "user-name", false},
		{"valid max length", makeString(64, 'a'), true},
		{"invalid too long", makeString(65, 'a'), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Identifier(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func makeString(n int, char byte) string {
	result := make([]byte, n)
	for i := range result {
		result[i] = char
	}
	return string(result)
}

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"simple", "users", "`users`"},
		{"with backtick", "user`name", "`user``name`"},
		{"empty", "", "``"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := QuoteIdentifier(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTableNameValidation(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{"valid", "users", nil},
		{"empty", "", ErrEmptyTableName},
		{"too long", makeString(65, 'a'), ErrTableNameTooLong},
		{"invalid chars", "user-name", ErrInvalidTableName},
		{"valid with underscore", "user_name", nil},
		{"valid max length", makeString(64, 'a'), nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := TableName(tt.input)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestTableNamesValidation(t *testing.T) {
	tests := []struct {
		name    string
		input   []string
		wantErr error
	}{
		{"valid", []string{"users", "posts"}, nil},
		{"empty list", []string{}, nil},
		{"one invalid", []string{"users", ""}, ErrEmptyTableName},
		{"all valid", []string{"a", "bb", "ccc"}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := TableNames(tt.input)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestColumnNameValidation(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{"valid", "user_id", nil},
		{"empty", "", ErrEmptyColumnName},
		{"too long", makeString(65, 'a'), ErrColumnNameTooLong},
		{"invalid chars", "user-name", ErrInvalidColumnName},
		{"valid with number", "user_id_123", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ColumnName(tt.input)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCountParens(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int
	}{
		{"empty", "", 0},
		{"no parens", "abc", 0},
		{"balanced", "(abc)", 0},
		{"unbalanced open", "(abc", 1},
		{"unbalanced close", "abc)", -1},
		{"nested", "((abc))", 0},
		{"more open", "((abc", 2},
		{"more close", "abc))", -2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CountParens(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestConstraintNamesEdgeCases(t *testing.T) {
	t.Run("empty constraint name is valid", func(t *testing.T) {
		table := &core.Table{
			Name: "users",
			Constraints: []*core.Constraint{
				{Name: "", Type: core.ConstraintPrimaryKey},
			},
		}
		err := ConstraintNames(table)
		assert.NoError(t, err)
	})
}

func TestIndexNamesEdgeCases(t *testing.T) {
	t.Run("empty index name is valid", func(t *testing.T) {
		table := &core.Table{
			Name: "users",
			Indexes: []*core.Index{
				{Name: "", Columns: []core.ColumnIndex{{Name: "id"}}},
			},
		}
		err := IndexNames(table)
		assert.NoError(t, err)
	})
}

func TestForeignKeysEdgeCases(t *testing.T) {
	t.Run("FK to non-existent table is handled elsewhere", func(t *testing.T) {
		tables := []*core.Table{
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
		}
		err := ForeignKeys(tables)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-existent table")
	})
}

func TestPartOfPrimaryKey(t *testing.T) {
	t.Run("column in primary key", func(t *testing.T) {
		table := &core.Table{
			Constraints: []*core.Constraint{
				{Type: core.ConstraintPrimaryKey, Columns: []string{"id", "name"}},
			},
		}
		assert.True(t, PartOfPrimaryKey(table, "id"))
		assert.True(t, PartOfPrimaryKey(table, "name"))
		assert.False(t, PartOfPrimaryKey(table, "email"))
	})

	t.Run("no primary key", func(t *testing.T) {
		table := &core.Table{
			Constraints: []*core.Constraint{},
		}
		assert.False(t, PartOfPrimaryKey(table, "id"))
	})
}
