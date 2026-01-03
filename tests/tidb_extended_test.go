package tests

import (
	"schemift/parser"
	"testing"

	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTiDBOptions(t *testing.T) {
	p := parser.NewSQLParser()

	sql := `
CREATE TABLE tidb_options (
    id INT PRIMARY KEY
) 
AUTO_ID_CACHE = 100
AUTO_RANDOM_BASE = 50
SHARD_ROW_ID_BITS = 4
PRE_SPLIT_REGIONS = 2
SECONDARY_ENGINE = 'TiFlash'
TTL = ` + "`" + `id` + "`" + ` + INTERVAL 1 DAY
TTL_ENABLE = 'ON'
TTL_JOB_INTERVAL = '1h'
IETF_QUOTES = 'ON'
STATS_COL_CHOICE = 'ALL'
STATS_SAMPLE_RATE = 0.5;
`

	db, err := p.ParseSchema(sql)
	require.NoError(t, err)
	require.Equal(t, 1, len(db.Tables))

	tbl := db.FindTable("tidb_options")
	require.NotNil(t, tbl)

	assert.Equal(t, uint64(100), tbl.AutoIdCache)
	assert.Equal(t, uint64(50), tbl.AutoRandomBase)
	assert.Equal(t, uint64(4), tbl.ShardRowID)
	assert.Equal(t, uint64(2), tbl.PreSplitRegion)
	assert.Equal(t, "TiFlash", tbl.SecondaryEngine)
	assert.Contains(t, tbl.TTL, "id")
	assert.Contains(t, tbl.TTL, "INTERVAL 1 DAY")
	assert.True(t, tbl.TTLEnable)
	assert.Equal(t, "1h", tbl.TTLJobInterval)
	assert.True(t, tbl.IetfQuotes)
	assert.Equal(t, "ALL", tbl.StatsColsChoice)
	assert.Equal(t, 0.5, tbl.StatsSampleRate)
}
