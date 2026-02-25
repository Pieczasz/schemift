package mysql

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"smf/internal/core"
)

type tableOptions struct {
	engine        string
	charset       string
	collate       string
	autoIncrement uint64
	rowFormat     string
	avgRowLength  uint64
	maxDataLength uint64
	checksum      uint64
	createOptions string
	comment       string
	tableType     string
}

type tidbTableOptions struct {
	shardRowID      uint64
	placementPolicy string
	affinity        string
	sequence        bool
}

type mysqlTableOptions struct {
	packKeys                 string
	insertMethod             string
	storageMedia             string
	connection               string
	password                 string
	union                    []string
	secondaryEngine          string
	tableChecksum            uint64
	engineAttribute          string
	secondaryEngineAttribute string
	pageCompressed           bool
	pageCompressionLevel     uint64
	ietfQuotes               bool
	nodegroup                uint64
	statsPersistent          string
	statsAutoRecalc          string
	statsSamplePages         string
	autoextendSize           string
}

type mariadbTableOptions struct {
	pageChecksum         uint64
	transactional        uint64
	encryptionKeyID      *int
	sequence             bool
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

		result[opts.engine] = opts
	}

	return result, rows.Err()
}

func queryTiDBTableOptions(ic *introspectCtx, placeholders []string, args []any) error {
	query := fmt.Sprintf(`
		SELECT
			table_name,
			tidb_row_id_sharding_info,
			tidb_placement_policy_name,
			tidb_affinity,
			tidb_table_mode
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

		err := rows.Scan(
			&tableName,
			&rowIDShardingInfo,
			&opts.placementPolicy,
			&opts.affinity,
			&opts.sequence,
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

		_ = tableName
		_ = opts
	}

	return rows.Err()
}
