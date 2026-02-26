package mysql

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"smf/internal/core"
)

type tableOptions struct {
	// Engine is the storage engine (e.g. "InnoDB", "MyISAM", "Aria").
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	engine string

	// Charset is the default character set for the table (e.g. "utf8mb4").
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	charset string

	// Collate is the default collation for the table (e.g. "utf8mb4_unicode_ci").
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	collate string

	// AutoIncrement sets the starting AUTO_INCREMENT value for the table.
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	autoIncrement uint64

	// RowFormat controls the physical row storage format (e.g. "DYNAMIC", "COMPRESSED", "COMPACT").
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	rowFormat string

	// AvgRowLength is a hint for the average row length in bytes.
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	avgRowLength uint64

	// MaxDataLength is the maximum length of data that can be stored in the table.
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	maxDataLength uint64

	// Checksum enables live table checksum computation (1 = enabled, 0 = disabled).
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	checksum uint64

	// Comment is the table comment.
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	comment string

	// TableType indicates the table type (e.g. "BASE TABLE", "VIEW", "SYSTEM VERSIONED").
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	tableType string

	// KeyBlockSize sets the page size in KB for compressed InnoDB tables.
	// MySQL: 5.5+
	// MariaDB: 10.0+
	// TiDB: All versions
	keyBlockSize uint64

	// DelayKeyWrite delays key-buffer flushes for MyISAM tables (1 = enabled).
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	delayKeyWrite uint64

	// Compression sets the page-level compression algorithm ("ZLIB", "LZ4", "NONE").
	// MySQL: 5.7+
	// MariaDB: 10.1+
	// TiDB: All versions
	compression string

	// Encryption enables transparent data encryption for the tablespace ("Y" or "N").
	// MySQL: 5.7+
	// MariaDB: 10.1.3+
	// TiDB: All versions
	encryption string

	// DataDirectory specifies the OS directory for the table data file.
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	dataDirectory string

	// IndexDirectory specifies the OS directory for the MyISAM index file.
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	indexDirectory string

	// StatsPersistent controls whether InnoDB table statistics are persisted to disk.
	// MySQL: 5.6+
	// MariaDB: 10.0+
	// TiDB: All versions
	statsPersistent string

	// StatsAutoRecalc controls whether InnoDB statistics are recalculated automatically.
	// MySQL: 5.6+
	// MariaDB: 10.0+
	// TiDB: All versions
	statsAutoRecalc string

	// StatsSamplePages sets the number of index pages sampled for statistics estimates.
	// MySQL: 5.6+
	// MariaDB: 10.0+
	// TiDB: All versions
	statsSamplePages string

	// MinRows is a hint for the minimum number of rows the table is expected to hold.
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	minRows uint64

	// MaxRows is a hint for the maximum number of rows the table is expected to hold.
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	maxRows uint64

	// PackKeys controls index packing for MyISAM tables ("0", "1", or "DEFAULT").
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	packKeys string

	// InsertMethod controls how rows are inserted into a MERGE table ("NO", "FIRST", "LAST").
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	insertMethod string

	// StorageMedia specifies the storage medium for NDB Cluster ("DISK" or "MEMORY").
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	storageMedia string

	// Connection is a connection string for a FEDERATED table.
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	connection string

	// Union lists the underlying MyISAM tables that form a MERGE table.
	// MySQL: All versions
	// MariaDB: All versions
	// TiDB: All versions
	union []string

	// createOptions holds the raw CREATE_OPTIONS string for parsing additional options.
	createOptions string
}

type tidbTableOptions struct {
	// ShardRowID enables implicit row-ID sharding to scatter hotspot writes across TiKV regions.
	// TiDB: 2.1.13+
	shardRowID uint64

	// PlacementPolicy assigns a placement policy that controls replica placement across datacenters.
	// TiDB: 5.3+
	placementPolicy string

	// Affinity sets the follower-read affinity label for tidb_replica_read.
	// TiDB: 4.0+
	affinity string

	// Sequence marks the table as backed by a TiDB SEQUENCE object.
	// TiDB: 4.0+
	sequence bool

	// AutoIDCache sets the auto-ID cache size per TiDB node to reduce ID allocation RPCs.
	// TiDB: 3.0+
	autoIDCache uint64

	// AutoRandomBase sets the starting shard bits base for AUTO_RANDOM columns.
	// TiDB: 3.1+
	autoRandomBase uint64

	// PreSplitRegion pre-splits the table into 2^n regions at creation time.
	// TiDB: 2.1.13+
	preSplitRegion uint64

	// TTL is the time-to-live expression for automatic row expiration.
	// TiDB: 6.5+
	ttl string

	// TTLEnable activates or suspends TTL-based row deletion for this table.
	// TiDB: 6.5+
	ttlEnable bool

	// TTLJobInterval controls how frequently the TTL background job runs.
	// TiDB: 6.5+
	ttlJobInterval string

	// StatsBuckets sets the number of histogram buckets used for table statistics.
	// TiDB: 5.0+
	statsBuckets uint64

	// StatsTopN sets the number of top-N values tracked in column statistics.
	// TiDB: 5.0+
	statsTopN uint64

	// StatsColsChoice controls which columns collect statistics.
	// TiDB: 6.1+
	statsColsChoice string

	// StatsColList is a comma-separated list of columns to collect statistics for.
	// TiDB: 6.1+
	statsColList string

	// StatsSampleRate is the sampling rate (0.0-1.0) used when collecting table statistics.
	// TiDB: 5.3+
	statsSampleRate float64
}

type mysqlTableOptions struct {
	// PackKeys controls index packing for MyISAM tables ("0", "1", or "DEFAULT").
	// MySQL: All versions
	packKeys string

	// InsertMethod controls how rows are inserted into a MERGE table ("NO", "FIRST", "LAST").
	// MySQL: All versions
	insertMethod string

	// StorageMedia specifies the storage medium for NDB Cluster ("DISK" or "MEMORY").
	// MySQL: All versions (NDB Cluster)
	storageMedia string

	// Connection is a connection string for a FEDERATED table.
	// MySQL: All versions
	connection string

	// Password is the password used by a FEDERATED table's connection string.
	// MySQL: All versions
	password string

	// Union lists the underlying MyISAM tables that form a MERGE table.
	// MySQL: All versions
	union []string

	// SecondaryEngine names the secondary engine for HeatWave / RAPID offload.
	// MySQL: 8.0.13+
	secondaryEngine string

	// TableChecksum enables per-row checksum stored in the table (NDB Cluster).
	// MySQL: All versions
	tableChecksum uint64

	// EngineAttribute is an opaque JSON string passed to the primary storage engine.
	// MySQL: 8.0.21+
	engineAttribute string

	// SecondaryEngineAttribute is an opaque JSON string passed to the secondary engine.
	// MySQL: 8.0.21+
	secondaryEngineAttribute string

	// PageCompressed enables InnoDB page-level compression.
	// MySQL: 5.7+
	pageCompressed bool

	// PageCompressionLevel sets the zlib compression level for page compression (1-9).
	// MySQL: 5.7+
	pageCompressionLevel uint64

	// IETFQuotes enables IETF-compliant quoting for CSV storage engine output.
	// MySQL: All versions
	ietfQuotes bool

	// Nodegroup assigns the table to an NDB Cluster node group.
	// MySQL: All versions (NDB Cluster)
	nodegroup uint64

	// AutoextendSize sets the InnoDB tablespace auto-extend chunk size.
	// MySQL: 8.0.23+
	autoextendSize string
}

type mariadbTableOptions struct {
	// PageChecksum enables page-level checksums for Aria storage engine tables.
	// MariaDB: 5.1+ (Aria)
	pageChecksum uint64

	// Transactional enables transactional support for Aria storage engine tables.
	// MariaDB: 5.1+ (Aria)
	transactional uint64

	// EncryptionKeyID specifies the encryption key ID for table encryption.
	// MariaDB: 10.1.3+
	encryptionKeyID *int

	// Sequence marks the table as a SEQUENCE object.
	// MariaDB: 10.3+
	sequence bool

	// WithSystemVersioning enables system-versioned (temporal) table.
	// MariaDB: 10.3.4+
	withSystemVersioning bool
}

func queryTableOptions(ic *introspectCtx, placeholders []string, args []any) (map[string]tableOptions, error) {
	if len(placeholders) == 0 {
		return nil, nil
	}

	result, err := queryBaseTableOptions(ic, placeholders, args)
	if err != nil {
		return nil, err
	}

	if ic.dialect == core.DialectTiDB {
		if err := queryTiDBTableOptions(ic, placeholders, args); err != nil {
			return nil, err
		}
	}

	return result, nil
}

func queryBaseTableOptions(ic *introspectCtx, placeholders []string, args []any) (map[string]tableOptions, error) {
	query := fmt.Sprintf(`
		SELECT
			table_name,
			engine,
			table_collation,
			auto_increment,
			row_format,
			avg_row_length,
			max_data_length,
			checksum,
			create_options,
			table_comment,
			table_type
		FROM information_schema.tables
		WHERE table_schema = DATABASE()
		  AND table_name IN (%s)
	`, strings.Join(placeholders, ","))

	rows, err := ic.db.QueryContext(ic.ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]tableOptions)
	for rows.Next() {
		var opts tableOptions
		var tableCollation sql.NullString

		err := rows.Scan(
			&opts.engine,
			&tableCollation,
			&opts.autoIncrement,
			&opts.rowFormat,
			&opts.avgRowLength,
			&opts.maxDataLength,
			&opts.checksum,
			&opts.createOptions,
			&opts.comment,
			&opts.tableType,
		)
		if err != nil {
			return nil, err
		}

		if tableCollation.Valid {
			parts := strings.SplitN(tableCollation.String, "_", 2)
			if len(parts) > 0 {
				opts.charset = parts[0]
				if len(parts) > 1 {
					opts.collate = tableCollation.String
				}
			}
		}

		opts.maxRows = opts.maxDataLength

		parseCreateOptions(&opts)

		result[opts.engine] = opts
	}

	return result, rows.Err()
}

func parseCreateOptions(opts *tableOptions) {
	if opts.createOptions == "" {
		return
	}

	pairs := parseKeyValuePairs(opts.createOptions)
	for key, value := range pairs {
		switch key {
		case "KEY_BLOCK_SIZE":
			if n, err := strconv.ParseUint(value, 10, 64); err == nil {
				opts.keyBlockSize = n
			}
		case "DELAY_KEY_WRITE":
			if n, err := strconv.ParseUint(value, 10, 64); err == nil {
				opts.delayKeyWrite = n
			}
		case "COMPRESSION":
			opts.compression = value
		case "ENCRYPTION":
			opts.encryption = value
		case "DATA DIRECTORY":
			opts.dataDirectory = value
		case "INDEX DIRECTORY":
			opts.indexDirectory = value
		case "STATS_PERSISTENT":
			opts.statsPersistent = value
		case "STATS_AUTO_RECALC":
			opts.statsAutoRecalc = value
		case "STATS_SAMPLE_PAGES":
			opts.statsSamplePages = value
		case "MIN_ROWS":
			if n, err := strconv.ParseUint(value, 10, 64); err == nil {
				opts.minRows = n
			}
		case "PACK_KEYS":
			opts.packKeys = value
		case "INSERT_METHOD":
			opts.insertMethod = value
		case "STORAGE_MEDIA":
			opts.storageMedia = value
		case "CONNECTION":
			opts.connection = value
		case "UNION":
			opts.union = strings.Split(value, ",")
		}
	}
}

func parseKeyValuePairs(input string) map[string]string {
	result := make(map[string]string)
	if input == "" {
		return result
	}

	var key, value strings.Builder
	inQuote := false
	quoteChar := rune(0)
	afterEquals := false

	for _, ch := range input {
		if ch == '"' || ch == '\'' {
			if !inQuote {
				inQuote = true
				quoteChar = ch
				continue
			}
			if ch == quoteChar {
				inQuote = false
				quoteChar = 0
				continue
			}
		}

		if !inQuote && ch == '=' {
			afterEquals = true
			continue
		}

		if !inQuote && ch == ' ' {
			if key.Len() > 0 && value.Len() > 0 {
				result[strings.ToUpper(key.String())] = value.String()
				key.Reset()
				value.Reset()
				afterEquals = false
			} else if key.Len() > 0 && value.Len() == 0 && afterEquals {
				// key= (empty value) - this shouldn't happen in valid CREATE_OPTIONS I guess.
			} else if key.Len() > 0 && !afterEquals {
				// This is key without =, like "partitioned"
				result[strings.ToUpper(key.String())] = ""
				key.Reset()
			}
			continue
		}

		if afterEquals || inQuote {
			value.WriteRune(ch)
		} else {
			key.WriteRune(ch)
		}
	}

	if key.Len() > 0 {
		if value.Len() > 0 {
			result[strings.ToUpper(key.String())] = value.String()
		} else if !afterEquals {
			result[strings.ToUpper(key.String())] = ""
		}
	}

	return result
}

func queryTiDBTableOptions(ic *introspectCtx, placeholders []string, args []any) error {
	query := fmt.Sprintf(`
		SELECT
			table_name,
			tidb_row_id_sharding_info,
			tidb_placement_policy_name,
			tidb_affinity,
			tidb_table_mode,
			create_options
		FROM information_schema.tables
		WHERE table_schema = DATABASE()
		  AND table_name IN (%s)
	`, strings.Join(placeholders, ","))

	rows, err := ic.db.QueryContext(ic.ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		var opts tidbTableOptions
		var rowIDShardingInfo string
		var createOptions string

		err := rows.Scan(
			&tableName,
			&rowIDShardingInfo,
			&opts.placementPolicy,
			&opts.affinity,
			&opts.sequence,
			&createOptions,
		)
		if err != nil {
			return err
		}

		if strings.Contains(rowIDShardingInfo, "SHARD_BITS=") {
			parts := strings.Split(rowIDShardingInfo, "SHARD_BITS=")
			if len(parts) > 1 {
				if n, err := strconv.ParseUint(parts[1], 10, 64); err == nil {
					opts.shardRowID = n
				}
			}
		}

		parseTiDBOptions(&opts, createOptions)
	}

	return rows.Err()
}

func parseTiDBOptions(opts *tidbTableOptions, createOptions string) {
	if createOptions == "" {
		return
	}

	pairs := parseKeyValuePairs(createOptions)
	for key, value := range pairs {
		switch key {
		case "AUTO_ID_CACHE":
			if n, err := strconv.ParseUint(value, 10, 64); err == nil {
				opts.autoIDCache = n
			}
		case "AUTO_RANDOM_BASE":
			if n, err := strconv.ParseUint(value, 10, 64); err == nil {
				opts.autoRandomBase = n
			}
		case "PRE_SPLIT_REGIONS":
			if n, err := strconv.ParseUint(value, 10, 64); err == nil {
				opts.preSplitRegion = n
			}
		case "TTL":
			opts.ttl = value
		case "TTL_ENABLE":
			opts.ttlEnable = strings.ToUpper(value) == "TRUE"
		case "TTL_JOB_INTERVAL":
			opts.ttlJobInterval = value
		case "STATS_BUCKETS":
			if n, err := strconv.ParseUint(value, 10, 64); err == nil {
				opts.statsBuckets = n
			}
		case "STATS_TOPN":
			if n, err := strconv.ParseUint(value, 10, 64); err == nil {
				opts.statsTopN = n
			}
		case "STATS_COLS_CHOICE":
			opts.statsColsChoice = value
		case "STATS_COL_LIST":
			opts.statsColList = value
		case "STATS_SAMPLE_RATE":
			if n, err := strconv.ParseFloat(value, 64); err == nil {
				opts.statsSampleRate = n
			}
		}
	}
}
