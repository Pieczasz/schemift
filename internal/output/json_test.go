package output

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"smf/internal/core"
	"smf/internal/diff"
	"smf/internal/migration"
)

func TestJSONFormatterFormatDiffNil(t *testing.T) {
	f := jsonFormatter{}
	out, err := f.FormatDiff(nil)
	require.NoError(t, err)
	assert.Contains(t, out, `"formatVersion"`)
	assert.Contains(t, out, `"format": "json"`)
}

func TestJSONFormatterFormatDiffEmpty(t *testing.T) {
	f := jsonFormatter{}
	out, err := f.FormatDiff(&diff.SchemaDiff{})
	require.NoError(t, err)
	assert.Contains(t, out, `"addedTables": 0`)
	assert.Contains(t, out, `"removedTables": 0`)
}

func TestJSONFormatterFormatDiffWithChanges(t *testing.T) {
	oldDB := &core.Database{
		Tables: []*core.Table{
			{Name: "users", Columns: []*core.Column{
				{Name: "id", TypeRaw: "INT", Type: core.DataTypeInt},
				{Name: "name", TypeRaw: "VARCHAR(255)", Type: core.DataTypeString, Nullable: true},
			}},
			{Name: "posts", Columns: []*core.Column{
				{Name: "id", TypeRaw: "INT", Type: core.DataTypeInt},
			}},
		},
	}
	newDB := &core.Database{
		Tables: []*core.Table{
			{Name: "users", Columns: []*core.Column{
				{Name: "id", TypeRaw: "INT", Type: core.DataTypeInt},
				{Name: "name", TypeRaw: "VARCHAR(255)", Type: core.DataTypeString, Nullable: false},
				{Name: "email", TypeRaw: "VARCHAR(255)", Type: core.DataTypeString},
			}},
			{Name: "comments", Columns: []*core.Column{
				{Name: "id", TypeRaw: "INT", Type: core.DataTypeInt},
			}},
		},
	}

	d := diff.Diff(oldDB, newDB, diff.DefaultOptions())
	f := jsonFormatter{}
	out, err := f.FormatDiff(d)
	require.NoError(t, err)

	assert.Contains(t, out, `"addedTables": 1`)
	assert.Contains(t, out, `"removedTables": 1`)
	assert.Contains(t, out, `"modifiedTables": 1`)
	assert.Contains(t, out, `"comments"`)
	assert.Contains(t, out, `"posts"`)
	assert.Contains(t, out, `"users"`)
}

func TestJSONFormatterFormatMigrationNil(t *testing.T) {
	f := jsonFormatter{}
	out, err := f.FormatMigration(nil)
	require.NoError(t, err)
	assert.Contains(t, out, `"formatVersion"`)
	assert.Contains(t, out, `"format": "json"`)
}

func TestJSONFormatterFormatMigrationWithOps(t *testing.T) {
	m := &migration.Migration{
		Operations: []core.Operation{
			{Kind: core.OperationSQL, SQL: "ALTER TABLE users ADD COLUMN email VARCHAR(255)"},
			{Kind: core.OperationBreaking, SQL: "column removed", Risk: core.RiskBreaking},
			{Kind: core.OperationNote, SQL: "review this change", Risk: core.RiskInfo},
		},
	}
	f := jsonFormatter{}
	out, err := f.FormatMigration(m)
	require.NoError(t, err)

	assert.Contains(t, out, `"sqlStatements": 1`)
	assert.Contains(t, out, `"breakingChanges": 1`)
	assert.Contains(t, out, `"notes": 1`)
	assert.Contains(t, out, "ALTER TABLE users ADD COLUMN email VARCHAR(255)")
}
