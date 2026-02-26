package mysql

import (
	"testing"
)

func TestParseKeyValuePairs(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected map[string]string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: map[string]string{},
		},
		{
			name:  "simple key value",
			input: "KEY_BLOCK_SIZE=8",
			expected: map[string]string{
				"KEY_BLOCK_SIZE": "8",
			},
		},
		{
			name:  "quoted value with single quotes",
			input: "COMPRESSION='ZLIB'",
			expected: map[string]string{
				"COMPRESSION": "ZLIB",
			},
		},
		{
			name:  "quoted value with double quotes",
			input: `ENCRYPTION="Y"`,
			expected: map[string]string{
				"ENCRYPTION": "Y",
			},
		},
		{
			name:  "multiple options",
			input: "KEY_BLOCK_SIZE=8 STATS_PERSISTENT=1",
			expected: map[string]string{
				"KEY_BLOCK_SIZE":   "8",
				"STATS_PERSISTENT": "1",
			},
		},
		{
			name:  "mixed quoted and unquoted",
			input: "STATS_PERSISTENT=1 COMPRESSION='LZ4'",
			expected: map[string]string{
				"STATS_PERSISTENT": "1",
				"COMPRESSION":      "LZ4",
			},
		},
		{
			name:  "row format dynamic",
			input: "row_format=DYNAMIC",
			expected: map[string]string{
				"ROW_FORMAT": "DYNAMIC",
			},
		},
		{
			name:  "partitioned",
			input: "partitioned",
			expected: map[string]string{
				"PARTITIONED": "",
			},
		},
		{
			name:  "stats options",
			input: "stats_persistent=1 stats_sample_pages=20",
			expected: map[string]string{
				"STATS_PERSISTENT":   "1",
				"STATS_SAMPLE_PAGES": "20",
			},
		},
		{
			name:  "encryption quoted",
			input: "ENCRYPTION='Y'",
			expected: map[string]string{
				"ENCRYPTION": "Y",
			},
		},
		{
			name:  "max rows and avg row length",
			input: "max_rows=1000000 avg_row_length=50",
			expected: map[string]string{
				"MAX_ROWS":       "1000000",
				"AVG_ROW_LENGTH": "50",
			},
		},
		{
			name:  "empty value",
			input: "standard_table",
			expected: map[string]string{
				"STANDARD_TABLE": "",
			},
		},
		{
			name:  "data directory with underscores (actual MySQL format)",
			input: "DATA_DIRECTORY='/var/lib/mysql/data'",
			expected: map[string]string{
				"DATA_DIRECTORY": "/var/lib/mysql/data",
			},
		},
		{
			name:  "index directory with underscores (actual MySQL format)",
			input: "INDEX_DIRECTORY='/var/lib/mysql/indexes'",
			expected: map[string]string{
				"INDEX_DIRECTORY": "/var/lib/mysql/indexes",
			},
		},
		{
			name:  "storage media with underscores (actual MySQL format)",
			input: "STORAGE_MEDIA=DISK",
			expected: map[string]string{
				"STORAGE_MEDIA": "DISK",
			},
		},
		{
			name:  "TiDB TTL quoted (how it should be stored)",
			input: "TTL='created_at + INTERVAL 90 DAY'",
			expected: map[string]string{
				"TTL": "created_at + INTERVAL 90 DAY",
			},
		},
		{
			name:  "real world examples from MySQL",
			input: "row_format=DYNAMIC",
			expected: map[string]string{
				"ROW_FORMAT": "DYNAMIC",
			},
		},
		{
			name:  "TiDB auto id cache",
			input: "AUTO_ID_CACHE=30000",
			expected: map[string]string{
				"AUTO_ID_CACHE": "30000",
			},
		},
		{
			name:  "TiDB pre split regions",
			input: "PRE_SPLIT_REGIONS=4",
			expected: map[string]string{
				"PRE_SPLIT_REGIONS": "4",
			},
		},
		{
			name:  "complex real world example",
			input: "stats_persistent=1 stats_sample_pages=20",
			expected: map[string]string{
				"STATS_PERSISTENT":   "1",
				"STATS_SAMPLE_PAGES": "20",
			},
		},
		{
			name:  "delay key write",
			input: "DELAY_KEY_WRITE=1",
			expected: map[string]string{
				"DELAY_KEY_WRITE": "1",
			},
		},
		{
			name:  "pack keys",
			input: "PACK_KEYS=1",
			expected: map[string]string{
				"PACK_KEYS": "1",
			},
		},
		{
			name:  "insert method",
			input: "INSERT_METHOD=LAST",
			expected: map[string]string{
				"INSERT_METHOD": "LAST",
			},
		},
		{
			name:  "min rows",
			input: "MIN_ROWS=100",
			expected: map[string]string{
				"MIN_ROWS": "100",
			},
		},
		{
			name:  "union for merge table",
			input: "UNION=(t1,t2)",
			expected: map[string]string{
				"UNION": "(t1,t2)",
			},
		},
		{
			name:  "connection for federated",
			input: "CONNECTION='mysql://user:pass@host:3306/db/tbl'",
			expected: map[string]string{
				"CONNECTION": "mysql://user:pass@host:3306/db/tbl",
			},
		},
		{
			name:  "stats auto recalc",
			input: "STATS_AUTO_RECALC=DEFAULT",
			expected: map[string]string{
				"STATS_AUTO_RECALC": "DEFAULT",
			},
		},
		{
			name:  "complex mixed options",
			input: "KEY_BLOCK_SIZE=8 COMPRESSION='ZLIB' ENCRYPTION='Y'",
			expected: map[string]string{
				"KEY_BLOCK_SIZE": "8",
				"COMPRESSION":    "ZLIB",
				"ENCRYPTION":     "Y",
			},
		},
		{
			name:  "value with equals sign inside quotes",
			input: "CONNECTION='host=localhost;port=3306'",
			expected: map[string]string{
				"CONNECTION": "host=localhost;port=3306",
			},
		},
		{
			name:  "value with spaces inside quotes",
			input: "TTL='created_at + INTERVAL 90 DAY'",
			expected: map[string]string{
				"TTL": "created_at + INTERVAL 90 DAY",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseKeyValuePairs(tt.input)

			if len(result) != len(tt.expected) {
				t.Errorf("parseKeyValuePairs(%q) returned %d pairs, expected %d", tt.input, len(result), len(tt.expected))
				t.Errorf("Got: %v", result)
				t.Errorf("Expected: %v", tt.expected)
				return
			}

			for key, expectedValue := range tt.expected {
				if result[key] != expectedValue {
					t.Errorf("parseKeyValuePairs(%q)[%s] = %q, expected %q", tt.input, key, result[key], expectedValue)
				}
			}
		})
	}
}

func TestParseKeyValuePairsEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected map[string]string
	}{
		{
			name:     "whitespace only",
			input:    "   ",
			expected: map[string]string{},
		},
		{
			name:  "leading whitespace",
			input: "  KEY=value",
			expected: map[string]string{
				"KEY": "value",
			},
		},
		{
			name:  "trailing whitespace",
			input: "KEY=value  ",
			expected: map[string]string{
				"KEY": "value",
			},
		},
		{
			name:  "multiple spaces between pairs",
			input: "KEY1=value1    KEY2=value2",
			expected: map[string]string{
				"KEY1": "value1",
				"KEY2": "value2",
			},
		},
		{
			name:  "value with numbers only",
			input: "KEY_BLOCK_SIZE=8192",
			expected: map[string]string{
				"KEY_BLOCK_SIZE": "8192",
			},
		},
		{
			name:  "value with leading zeros",
			input: "STATS_SAMPLE_PAGES=0010",
			expected: map[string]string{
				"STATS_SAMPLE_PAGES": "0010",
			},
		},
		{
			name:  "case insensitive keys",
			input: "stats_persistent=1",
			expected: map[string]string{
				"STATS_PERSISTENT": "1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseKeyValuePairs(tt.input)

			if len(result) != len(tt.expected) {
				t.Errorf("parseKeyValuePairs(%q) returned %d pairs, expected %d", tt.input, len(result), len(tt.expected))
				t.Errorf("Got: %v", result)
				t.Errorf("Expected: %v", tt.expected)
				return
			}

			for key, expectedValue := range tt.expected {
				if result[key] != expectedValue {
					t.Errorf("parseKeyValuePairs(%q)[%s] = %q, expected %q", tt.input, key, result[key], expectedValue)
				}
			}
		})
	}
}
