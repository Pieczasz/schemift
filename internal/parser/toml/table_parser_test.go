package toml

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseEmptyTable(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "empty"
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	assert.Error(t, err)
}

func TestParseDuplicateTableName(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	assert.Error(t, err)
}

func TestParseTableOptions(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [tables.options]
  tablespace     = "ts1"

  [tables.options.mysql]
  engine         = "InnoDB"
  charset        = "utf8mb4"
  collate        = "utf8mb4_general_ci"
  row_format     = "COMPRESSED"
  compression    = "zlib"
  encryption     = "Y"
  key_block_size = 8

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	opts := db.Tables[0].Options
	assert.Equal(t, "ts1", opts.Tablespace)
	require.NotNil(t, opts.MySQL)
	assert.Equal(t, "InnoDB", opts.MySQL.Engine)
	assert.Equal(t, "utf8mb4", opts.MySQL.Charset)
	assert.Equal(t, "utf8mb4_general_ci", opts.MySQL.Collate)
	assert.Equal(t, "COMPRESSED", opts.MySQL.RowFormat)
	assert.Equal(t, "zlib", opts.MySQL.Compression)
	assert.Equal(t, "Y", opts.MySQL.Encryption)
	assert.Equal(t, uint64(8), opts.MySQL.KeyBlockSize)
}

func TestParseTimestampsInjection(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [tables.timestamps]
  enabled = true

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	require.NotNil(t, tbl.Timestamps)
	assert.True(t, tbl.Timestamps.Enabled)

	// 1 declared + 2 injected = 3.
	assert.Len(t, tbl.Columns, 3)

	createdAt := tbl.FindColumn("created_at")
	require.NotNil(t, createdAt)
	assert.Equal(t, "timestamp", createdAt.RawType)
	require.NotNil(t, createdAt.DefaultValue)
	assert.Equal(t, "CURRENT_TIMESTAMP", *createdAt.DefaultValue)

	updatedAt := tbl.FindColumn("updated_at")
	require.NotNil(t, updatedAt)
	assert.Equal(t, "timestamp", updatedAt.RawType)
	require.NotNil(t, updatedAt.DefaultValue)
	assert.Equal(t, "CURRENT_TIMESTAMP", *updatedAt.DefaultValue)
	require.NotNil(t, updatedAt.OnUpdate)
	assert.Equal(t, "CURRENT_TIMESTAMP", *updatedAt.OnUpdate)
}

func TestParseTimestampsCustomColumnNames(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [tables.timestamps]
  enabled        = true
  created_column = "inserted_at"
  updated_column = "modified_at"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	assert.Len(t, tbl.Columns, 3)

	assert.NotNil(t, tbl.FindColumn("inserted_at"))
	assert.NotNil(t, tbl.FindColumn("modified_at"))
	assert.Nil(t, tbl.FindColumn("created_at"))
	assert.Nil(t, tbl.FindColumn("updated_at"))
}

func TestParseTimestampsSkipIfColumnsExist(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [tables.timestamps]
  enabled = true

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [[tables.columns]]
  name    = "created_at"
  type    = "timestamp"
  default = "CUSTOM_VALUE"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	// created_at already exists -> not injected again.
	// updated_at doesn't exist -> injected.
	assert.Len(t, tbl.Columns, 3)

	createdAt := tbl.FindColumn("created_at")
	require.NotNil(t, createdAt)
	require.NotNil(t, createdAt.DefaultValue)
	assert.Equal(t, "CUSTOM_VALUE", *createdAt.DefaultValue, "existing column should not be overwritten")
}

func TestParseTimestampsDisabled(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [tables.timestamps]
  enabled = false

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	assert.Len(t, tbl.Columns, 1, "timestamps disabled -> no injection")
}

func TestParseTimestampsSameColumnName(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [tables.timestamps]
  enabled        = true
  created_column = "ts"
  updated_column = "ts"
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "same name")
	assert.Contains(t, err.Error(), "ts")
}

func TestParseTimestampsDefaultsSameColumnName(t *testing.T) {
	// Both default to the same name if one overrides to match the other's default.
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [tables.timestamps]
  enabled        = true
  created_column = "updated_at"
`
	p := NewParser()
	_, err := p.Parse(strings.NewReader(schema))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "same name")
}

func TestParseTimestampsDistinctColumnsValid(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [tables.timestamps]
  enabled        = true
  created_column = "inserted_at"
  updated_column = "modified_at"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	assert.NotNil(t, tbl.FindColumn("inserted_at"))
	assert.NotNil(t, tbl.FindColumn("modified_at"))
}

func TestParseTableWithoutPK(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "logs"

  [[tables.columns]]
  name = "message"
  type = "text"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)

	tbl := db.Tables[0]
	assert.Nil(t, tbl.PrimaryKey())
}

func TestParseDistinctColumnsValid(t *testing.T) {
	const schema = `
[database]
name = "testdb"

[[tables]]
name = "items"

  [[tables.columns]]
  name = "id"
  type = "int"
  primary_key = true

  [[tables.columns]]
  name = "name"
  type = "varchar(255)"

  [[tables.columns]]
  name = "code"
  type = "varchar(50)"
`
	p := NewParser()
	db, err := p.Parse(strings.NewReader(schema))
	require.NoError(t, err)
	assert.Len(t, db.Tables[0].Columns, 3)
}
