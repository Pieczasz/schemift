package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewParserAndInitialization(t *testing.T) {
	p := NewParser()
	assert.NotNil(t, p)
	assert.NotNil(t, p.p)
}

func TestParseAndBasicCreateTable(t *testing.T) {
	sql := `CREATE TABLE users (
		id INT NOT NULL,
		name VARCHAR(255)
	);`

	p := NewParser()
	db, err := p.Parse(sql)
	require.NoError(t, err)
	require.NotNil(t, db)
	require.Len(t, db.Tables, 1)

	table := db.Tables[0]
	assert.Equal(t, "users", table.Name)
	require.Len(t, table.Columns, 2)
	assert.Equal(t, "id", table.Columns[0].Name)
	assert.False(t, table.Columns[0].Nullable)
}

func TestParseAndMultipleTables(t *testing.T) {
	sql := `CREATE TABLE t2 (id INT); CREATE TABLE t1 (id INT);`

	p := NewParser()
	db, err := p.Parse(sql)
	require.NoError(t, err)
	require.Len(t, db.Tables, 2)
	assert.Equal(t, "t2", db.Tables[0].Name)
	assert.Equal(t, "t1", db.Tables[1].Name)
}

func TestParseAndInvalidSQL(t *testing.T) {
	sql := `CREATE TABLE users (`

	p := NewParser()
	db, err := p.Parse(sql)
	assert.Error(t, err)
	assert.Nil(t, db)
	assert.Contains(t, err.Error(), "SQL parse error")
}

func TestParseAndDuplicateTableValidation(t *testing.T) {
	sql := `CREATE TABLE users (id INT); CREATE TABLE users (id INT);`

	p := NewParser()
	db, err := p.Parse(sql)
	assert.Error(t, err)
	assert.Nil(t, db)
	assert.Contains(t, err.Error(), "schema validation failed")
}
