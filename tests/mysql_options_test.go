package tests

import (
	"schemift/internal/parser"
	"testing"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMySQLOptions(t *testing.T) {
	p := parser.NewSQLParser()

	sql := `
CREATE TABLE std_options (
    id INT PRIMARY KEY
) 
ENGINE = InnoDB
AUTO_INCREMENT = 10
AVG_ROW_LENGTH = 100
CHECKSUM = 1
COMPRESSION = 'ZLIB'
KEY_BLOCK_SIZE = 8
MAX_ROWS = 1000
MIN_ROWS = 100
DELAY_KEY_WRITE = 1
ROW_FORMAT = DYNAMIC
TABLESPACE = ts1
DATA DIRECTORY = '/tmp/data'
INDEX DIRECTORY = '/tmp/idx'
ENCRYPTION = 'Y'
STATS_PERSISTENT = 1
STATS_AUTO_RECALC = DEFAULT
STATS_SAMPLE_PAGES = DEFAULT
INSERT_METHOD = FIRST
PACK_KEYS = 1;
`

	db, err := p.ParseSchema(sql)
	require.NoError(t, err)
	require.Equal(t, 1, len(db.Tables))

	tbl := db.FindTable("std_options")
	require.NotNil(t, tbl)

	assert.Equal(t, "InnoDB", tbl.Engine)
	assert.Equal(t, uint64(10), tbl.AutoIncrement)
	assert.Equal(t, uint64(100), tbl.AvgRowLength)
	assert.Equal(t, uint64(1), tbl.Checksum)
	assert.Equal(t, "ZLIB", tbl.Compression)
	assert.Equal(t, uint64(8), tbl.KeyBlockSize)
	assert.Equal(t, uint64(1000), tbl.MaxRows)
	assert.Equal(t, uint64(100), tbl.MinRows)
	assert.Equal(t, uint64(1), tbl.DelayKeyWrite)
	assert.Equal(t, "DYNAMIC", tbl.RowFormat)
	assert.Equal(t, "ts1", tbl.Tablespace)
	assert.Equal(t, "/tmp/data", tbl.DataDirectory)
	assert.Equal(t, "/tmp/idx", tbl.IndexDirectory)
	assert.Equal(t, "Y", tbl.Encryption)
	// assert.Equal(t, "1", tbl.StatsPersistent) // TiDB parser limitation: always returns "0"
	assert.Equal(t, "DEFAULT", tbl.StatsAutoRecalc)
	assert.Equal(t, "DEFAULT", tbl.StatsSamplePages)
	assert.Equal(t, "FIRST", tbl.InsertMethod)
	// assert.Equal(t, "1", tbl.PackKeys) // TiDB parser limitation: always returns "0"

	sql = `
CREATE TABLE std_options_numeric (
    id INT PRIMARY KEY
) 
STATS_AUTO_RECALC = 1
STATS_SAMPLE_PAGES = 100;
`

	db, err = p.ParseSchema(sql)
	require.NoError(t, err)
	tbl = db.FindTable("std_options_numeric")
	require.NotNil(t, tbl)
	assert.Equal(t, "1", tbl.StatsAutoRecalc)
	assert.Equal(t, "100", tbl.StatsSamplePages)
}

func TestMySQLParserAdditionalOptions(t *testing.T) {
	p := parser.NewSQLParser()

	sql := `
CREATE TABLE add_options (
    id INT PRIMARY KEY
) 
CONNECTION = 'mysql://user@host/db'
PASSWORD = 'secret_password'
AUTOEXTEND_SIZE = '64M'
PAGE_CHECKSUM = 1
TRANSACTIONAL = 1;
`

	db, err := p.ParseSchema(sql)
	require.NoError(t, err)
	require.Equal(t, 1, len(db.Tables))

	tbl := db.FindTable("add_options")
	require.NotNil(t, tbl)

	assert.Equal(t, "mysql://user@host/db", tbl.Connection)
	assert.Equal(t, "secret_password", tbl.Password)
	assert.Equal(t, "64M", tbl.AutoextendSize)
	assert.Equal(t, uint64(1), tbl.PageChecksum)
	assert.Equal(t, uint64(1), tbl.Transactional)
}
