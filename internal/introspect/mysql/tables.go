package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	"smf/internal/core"
)

//revive:disable:function-length Reason: necessary for comprehensive table introspection
//revive:disable:cyclomatic Reason: necessary for comprehensive table introspection
func introspectTables(ic *introspectCtx, d *core.Database) error {
	rows, err := ic.db.QueryContext(ic.ctx, `
		SELECT 
			TABLE_NAME,
			TABLE_COMMENT,
			ENGINE,
			AUTO_INCREMENT,
			TABLE_COLLATION,
			ROW_FORMAT,
			AVG_ROW_LENGTH,
			MAX_ROWS,
			MIN_ROWS,
			CHECKSUM,
			DELAY_KEY_WRITE,
			KEY_BLOCK_SIZE,
			COMPRESSION,
			ENCRYPTION,
			PACK_KEYS,
			DATA_DIRECTORY,
			INDEX_DIRECTORY,
			INSERT_METHOD,
			STORAGE_MEDIA,
			STATS_PERSISTENT,
			STATS_AUTO_RECALC,
			STATS_SAMPLE_PAGES,
			CONNECTION,
			PASSWORD,
			AUTOEXTEND_SIZE,
			UNION,
			SECONDARY_ENGINE,
			TABLE_CHECKSUM,
			ENGINE_ATTRIBUTE,
			SECONDARY_ENGINE_ATTRIBUTE,
			PAGE_COMPRESSED,
			PAGE_COMPRESSION_LEVEL,
			IETF_QUOTES,
			NODEGROUP
		FROM information_schema.TABLES 
		WHERE TABLE_SCHEMA = ?
		AND TABLE_TYPE = 'BASE TABLE'
	`, d.Name)
	if err != nil {
		return fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		t := &core.Table{
			Options: core.TableOptions{
				MySQL:   &core.MySQLTableOptions{},
				MariaDB: &core.MariaDBTableOptions{},
			},
		}

		var (
			autoIncrement            sql.NullInt64
			avgRowLength             sql.NullInt64
			maxRows                  sql.NullInt64
			minRows                  sql.NullInt64
			checksum                 sql.NullInt64
			delayKeyWrite            sql.NullInt64
			keyBlockSize             sql.NullInt64
			tableChecksum            sql.NullInt64
			nodegroup                sql.NullInt64
			pageCompressed           sql.NullInt64
			pageCompression          sql.NullInt64
			iETFQuotes               sql.NullInt64
			unionStr                 sql.NullString
			connection               sql.NullString
			password                 sql.NullString
			autoextendSize           sql.NullString
			engineAttribute          sql.NullString
			secondaryEngineAttribute sql.NullString
		)

		err := rows.Scan(
			&t.Name,
			&t.Comment,
			&t.Options.MySQL.Engine,
			&autoIncrement,
			&t.Options.MySQL.Collate,
			&t.Options.MySQL.RowFormat,
			&avgRowLength,
			&maxRows,
			&minRows,
			&checksum,
			&delayKeyWrite,
			&keyBlockSize,
			&t.Options.MySQL.Compression,
			&t.Options.MySQL.Encryption,
			&t.Options.MySQL.PackKeys,
			&t.Options.MySQL.DataDirectory,
			&t.Options.MySQL.IndexDirectory,
			&t.Options.MySQL.InsertMethod,
			&t.Options.MySQL.StorageMedia,
			&t.Options.MySQL.StatsPersistent,
			&t.Options.MySQL.StatsAutoRecalc,
			&t.Options.MySQL.StatsSamplePages,
			&connection,
			&password,
			&autoextendSize,
			&unionStr,
			&t.Options.MySQL.SecondaryEngine,
			&tableChecksum,
			&engineAttribute,
			&secondaryEngineAttribute,
			&pageCompressed,
			&pageCompression,
			&iETFQuotes,
			&nodegroup,
		)
		if err != nil {
			return fmt.Errorf("failed to scan table row: %w", err)
		}

		if autoIncrement.Valid {
			t.Options.MySQL.AutoIncrement = uint64(autoIncrement.Int64)
		}
		if avgRowLength.Valid {
			t.Options.MySQL.AvgRowLength = uint64(avgRowLength.Int64)
		}
		if maxRows.Valid {
			t.Options.MySQL.MaxRows = uint64(maxRows.Int64)
		}
		if minRows.Valid {
			t.Options.MySQL.MinRows = uint64(minRows.Int64)
		}
		if checksum.Valid {
			t.Options.MySQL.Checksum = uint64(checksum.Int64)
		}
		if delayKeyWrite.Valid {
			t.Options.MySQL.DelayKeyWrite = uint64(delayKeyWrite.Int64)
		}
		if keyBlockSize.Valid {
			t.Options.MySQL.KeyBlockSize = uint64(keyBlockSize.Int64)
		}
		if tableChecksum.Valid {
			t.Options.MySQL.TableChecksum = uint64(tableChecksum.Int64)
		}
		if nodegroup.Valid {
			t.Options.MySQL.Nodegroup = uint64(nodegroup.Int64)
		}
		if pageCompressed.Valid && pageCompressed.Int64 == 1 {
			t.Options.MySQL.PageCompressed = true
		}
		if pageCompression.Valid {
			t.Options.MySQL.PageCompressionLevel = uint64(pageCompression.Int64)
		}
		if iETFQuotes.Valid && iETFQuotes.Int64 == 1 {
			t.Options.MySQL.IETFQuotes = true
		}
		if unionStr.Valid && unionStr.String != "" {
			t.Options.MySQL.Union = strings.Split(unionStr.String, ",")
		}
		if connection.Valid {
			t.Options.MySQL.Connection = connection.String
		}
		if password.Valid {
			t.Options.MySQL.Password = password.String
		}
		if autoextendSize.Valid {
			t.Options.MySQL.AutoextendSize = autoextendSize.String
		}
		if engineAttribute.Valid {
			t.Options.MySQL.EngineAttribute = engineAttribute.String
		}
		if secondaryEngineAttribute.Valid {
			t.Options.MySQL.SecondaryEngineAttribute = secondaryEngineAttribute.String
		}

		t.Options.MySQL.Charset = extractCharsetFromCollation(t.Options.MySQL.Collate)

		err = introspectColumns(ic, d.Name, t)
		if err != nil {
			return fmt.Errorf("failed to introspect columns for table %s: %w", t.Name, err)
		}

		err = introspectConstraints(ic, d.Name, t)
		if err != nil {
			return fmt.Errorf("failed to introspect constraints for table %s: %w", t.Name, err)
		}

		err = introspectIndexes(ic, d.Name, t)
		if err != nil {
			return fmt.Errorf("failed to introspect indexes for table %s: %w", t.Name, err)
		}

		d.Tables = append(d.Tables, t)
	}

	return rows.Err()
}

func extractCharsetFromCollation(collation string) string {
	if collation == "" {
		return ""
	}
	parts := strings.Split(collation, "_")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

func introspectColumns(ic *introspectCtx, schema string, t *core.Table) error {
	rows, err := ic.db.QueryContext(ic.ctx, `
		SELECT 
			COLUMN_NAME,
			COLUMN_TYPE,
			IS_NULLABLE,
			COLUMN_KEY,
			COLUMN_DEFAULT,
			EXTRA,
			COLUMN_COMMENT,
			CHARACTER_SET_NAME,
			COLLATION_NAME,
			COLUMN_FORMAT,
			STORAGE,
			ENGINE_ATTRIBUTE,
			SECONDARY_ENGINE_ATTRIBUTE,
			INVISIBLE
		FROM information_schema.COLUMNS 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`, schema, t.Name)
	if err != nil {
		return fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		c := &core.Column{
			MySQL: &core.MySQLColumnOptions{},
		}

		var (
			nullable            string
			columnKey           string
			extra               string
			defaultValue        sql.NullString
			columnFormat        sql.NullString
			storage             sql.NullString
			engineAttr          sql.NullString
			secondaryEngineAttr sql.NullString
			invisible           string
		)

		err := rows.Scan(
			&c.Name,
			&c.RawType,
			&nullable,
			&columnKey,
			&defaultValue,
			&extra,
			&c.Comment,
			&c.Charset,
			&c.Collate,
			&columnFormat,
			&storage,
			&engineAttr,
			&secondaryEngineAttr,
			&invisible,
		)
		if err != nil {
			return fmt.Errorf("failed to scan column row: %w", err)
		}

		c.Nullable = nullable == "YES"
		c.Type = core.NormalizeDataType(c.RawType)

		if columnKey == "PRI" {
			c.PrimaryKey = true
		}
		if columnKey == "UNI" {
			c.Unique = true
		}

		if strings.Contains(extra, "auto_increment") {
			c.AutoIncrement = true
		}
		if strings.Contains(extra, "VIRTUAL") {
			c.IsGenerated = true
			c.GenerationStorage = core.GenerationVirtual
		}
		if strings.Contains(extra, "STORED") {
			c.IsGenerated = true
			c.GenerationStorage = core.GenerationStored
		}

		if defaultValue.Valid {
			c.DefaultValue = &defaultValue.String
		}

		if strings.Contains(strings.ToLower(extra), "on update") {
			onUpdateStr := extractOnUpdate(extra)
			if onUpdateStr != "" {
				c.OnUpdate = &onUpdateStr
			}
		}

		if columnFormat.Valid {
			c.MySQL.ColumnFormat = columnFormat.String
		}
		if storage.Valid {
			c.MySQL.Storage = storage.String
		}
		if engineAttr.Valid {
			c.MySQL.PrimaryEngineAttribute = engineAttr.String
		}
		if secondaryEngineAttr.Valid {
			c.MySQL.SecondaryEngineAttribute = secondaryEngineAttr.String
		}
		if invisible == "YES" {
			c.Invisible = true
		}

		t.Columns = append(t.Columns, c)
	}

	return rows.Err()
}

func extractOnUpdate(extra string) string {
	lower := strings.ToLower(extra)
	if idx := strings.Index(lower, "on update"); idx != -1 {
		return strings.TrimSpace(extra[idx+len("on update"):])
	}
	return ""
}

func introspectConstraints(ic *introspectCtx, schema string, t *core.Table) error {
	rows, err := ic.db.QueryContext(ic.ctx, `
		SELECT 
			CONSTRAINT_NAME,
			CONSTRAINT_TYPE,
			TABLE_NAME
		FROM information_schema.TABLE_CONSTRAINTS 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`, schema, t.Name)
	if err != nil {
		return fmt.Errorf("failed to query constraints: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var constraintName, constraintType, tableName string
		err := rows.Scan(&constraintName, &constraintType, &tableName)
		if err != nil {
			return fmt.Errorf("failed to scan constraint row: %w", err)
		}

		c := &core.Constraint{
			Name: constraintName,
		}

		switch constraintType {
		case "PRIMARY KEY":
			c.Type = core.ConstraintPrimaryKey
		case "UNIQUE":
			c.Type = core.ConstraintUnique
		case "FOREIGN KEY":
			c.Type = core.ConstraintForeignKey
		case "CHECK":
			c.Type = core.ConstraintCheck
		}

		if c.Type == core.ConstraintPrimaryKey || c.Type == core.ConstraintUnique {
			colRows, err := ic.db.QueryContext(ic.ctx, `
				SELECT COLUMN_NAME 
				FROM information_schema.STATISTICS 
				WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ? AND NON_UNIQUE = 0
				ORDER BY SEQ_IN_INDEX
			`, schema, tableName, constraintName)
			if err != nil {
				return fmt.Errorf("failed to query constraint columns: %w", err)
			}
			defer colRows.Close()

			for colRows.Next() {
				var colName string
				if err := colRows.Scan(&colName); err != nil {
					return fmt.Errorf("failed to scan constraint column: %w", err)
				}
				c.Columns = append(c.Columns, colName)
			}
			if err := colRows.Err(); err != nil {
				return fmt.Errorf("error iterating constraint columns: %w", err)
			}
		}

		if c.Type == core.ConstraintForeignKey {
			fkRows, err := ic.db.QueryContext(ic.ctx, `
				SELECT
					kcu.COLUMN_NAME,
					kcu.REFERENCED_TABLE_NAME,
					kcu.REFERENCED_COLUMN_NAME,
					rc.DELETE_RULE,
					rc.UPDATE_RULE
				FROM information_schema.KEY_COLUMN_USAGE kcu
				JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
					ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
					AND kcu.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
				WHERE kcu.TABLE_SCHEMA = ?
					AND kcu.TABLE_NAME = ?
					AND kcu.CONSTRAINT_NAME = ?
			`, schema, tableName, constraintName)
			if err != nil {
				return fmt.Errorf("failed to query foreign key: %w", err)
			}
			defer fkRows.Close()

			for fkRows.Next() {
				var colName, refTable, refCol string
				var onDelete, onUpdate string
				err := fkRows.Scan(&colName, &refTable, &refCol, &onDelete, &onUpdate)
				if err != nil {
					return fmt.Errorf("failed to scan foreign key: %w", err)
				}
				c.Columns = append(c.Columns, colName)
				c.ReferencedTable = refTable
				c.ReferencedColumns = append(c.ReferencedColumns, refCol)
				c.OnDelete = parseReferentialAction(onDelete)
				c.OnUpdate = parseReferentialAction(onUpdate)
			}
			if err := fkRows.Err(); err != nil {
				return fmt.Errorf("error iterating foreign key rows: %w", err)
			}
		}

		t.Constraints = append(t.Constraints, c)
	}

	return rows.Err()
}

func parseReferentialAction(action string) core.ReferentialAction {
	switch strings.ToUpper(action) {
	case "CASCADE":
		return core.RefActionCascade
	case "SET NULL":
		return core.RefActionSetNull
	case "RESTRICT":
		return core.RefActionRestrict
	case "NO ACTION":
		return core.RefActionNoAction
	case "SET DEFAULT":
		return core.RefActionSetDefault
	default:
		return core.RefActionNone
	}
}

//revive:disable:cyclomatic Reason: necessary for comprehensive index introspection
func introspectIndexes(ic *introspectCtx, schema string, t *core.Table) error {
	rows, err := ic.db.QueryContext(ic.ctx, `
		SELECT 
			INDEX_NAME,
			NON_UNIQUE,
			INDEX_TYPE,
			COMMENT,
			VISIBLE
		FROM information_schema.STATISTICS 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND SEQ_IN_INDEX = 1
	`, schema, t.Name)
	if err != nil {
		return fmt.Errorf("failed to query indexes: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var indexName string
		var nonUnique int
		var indexType, comment, visible string

		err := rows.Scan(&indexName, &nonUnique, &indexType, &comment, &visible)
		if err != nil {
			return fmt.Errorf("failed to scan index row: %w", err)
		}

		if indexName == "PRIMARY" {
			continue
		}

		idx := &core.Index{
			Name:       indexName,
			Unique:     nonUnique == 0,
			Type:       parseIndexType(indexType),
			Comment:    comment,
			Visibility: parseIndexVisibility(visible),
		}

		colRows, err := ic.db.QueryContext(ic.ctx, `
			SELECT COLUMN_NAME, SUB_PART, COLLATION
			FROM information_schema.STATISTICS 
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ?
			ORDER BY SEQ_IN_INDEX
		`, schema, t.Name, indexName)
		if err != nil {
			return fmt.Errorf("failed to query index columns: %w", err)
		}
		defer colRows.Close()

		for colRows.Next() {
			var colName string
			var subPart int
			var collation string
			err := colRows.Scan(&colName, &subPart, &collation)
			if err != nil {
				return fmt.Errorf("failed to scan index column: %w", err)
			}

			colIdx := core.ColumnIndex{
				Name:   colName,
				Length: subPart,
			}
			if collation == "D" {
				colIdx.Order = core.SortDesc
			} else {
				colIdx.Order = core.SortAsc
			}
			idx.Columns = append(idx.Columns, colIdx)
		}
		if err := colRows.Err(); err != nil {
			return fmt.Errorf("error iterating index columns: %w", err)
		}

		t.Indexes = append(t.Indexes, idx)
	}

	return rows.Err()
}

func parseIndexType(indexType string) core.IndexType {
	switch strings.ToUpper(indexType) {
	case "BTREE":
		return core.IndexTypeBTree
	case "HASH":
		return core.IndexTypeHash
	case "FULLTEXT":
		return core.IndexTypeFullText
	case "SPATIAL":
		return core.IndexTypeSpatial
	default:
		return core.IndexTypeBTree
	}
}

func parseIndexVisibility(visible string) core.IndexVisibility {
	switch strings.ToUpper(visible) {
	case "YES":
		return core.IndexVisible
	case "NO":
		return core.IndexInvisible
	default:
		return core.IndexVisible
	}
}
