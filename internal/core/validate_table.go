package core

import (
	"errors"
	"fmt"
	"regexp"
)

func validateDuplicateTableNames(tables []*Table) error {
	seenTables := make(map[string]bool, len(tables))
	for _, table := range tables {
		if seenTables[table.Name] {
			return fmt.Errorf("duplicate table name %q", table.Name)
		}
		seenTables[table.Name] = true
	}
	return nil
}

func prevalidateAndSynthesizeTables(tables []*Table) error {
	for _, table := range tables {
		if err := validatePKConflict(table); err != nil {
			return fmt.Errorf("table %q: %w", table.Name, err)
		}
		synthesizeConstraints(table)
	}
	return nil
}

func validateAllTables(tables []*Table, rules *ValidationRules, nameRe *regexp.Regexp) error {
	for _, table := range tables {
		if err := validateTable(table, rules, nameRe); err != nil {
			return fmt.Errorf("table %q: %w", table.Name, err)
		}
	}
	return nil
}

// validateTable checks a single table for structural correctness.
func validateTable(table *Table, rules *ValidationRules, nameRe *regexp.Regexp) error {
	if err := validateName(table.Name, "table", rules, nameRe, true); err != nil {
		return err
	}

	// TODO: validate this field (table.Comment)
	if err := validateTableOptions(&table.Options); err != nil {
		return err
	}

	if len(table.Columns) == 0 {
		return errors.New("table has no columns")
	}

	seenCols := make(map[string]bool, len(table.Columns))
	for _, col := range table.Columns {
		if seenCols[col.Name] {
			return fmt.Errorf("duplicate column name %q", col.Name)
		}
		seenCols[col.Name] = true
	}

	for _, col := range table.Columns {
		if err := validateColumn(col, rules, nameRe); err != nil {
			return fmt.Errorf("column %q: %w", col.Name, err)
		}
	}

	if err := validateConstraints(table); err != nil {
		return err
	}

	if err := validateTimestamps(table); err != nil {
		return err
	}

	if err := validateIndexes(table); err != nil {
		return err
	}

	return nil
}

func validateTableOptions(opt *TableOptions) error {
	// TODO: validate this field (opt.Tablespace)

	if opt.MySQL != nil {
		// TODO: validate this field (opt.MySQL.Engine)
		// TODO: validate this field (opt.MySQL.Charset)
		// TODO: validate this field (opt.MySQL.Collate)
		// TODO: validate this field (opt.MySQL.AutoIncrement)
		// TODO: validate this field (opt.MySQL.RowFormat)
		// TODO: validate this field (opt.MySQL.AvgRowLength)
		// TODO: validate this field (opt.MySQL.KeyBlockSize)
		// TODO: validate this field (opt.MySQL.MaxRows)
		// TODO: validate this field (opt.MySQL.MinRows)
		// TODO: validate this field (opt.MySQL.Checksum)
		// TODO: validate this field (opt.MySQL.DelayKeyWrite)
		// TODO: validate this field (opt.MySQL.Compression)
		// TODO: validate this field (opt.MySQL.Encryption)
		// TODO: validate this field (opt.MySQL.PackKeys)
		// TODO: validate this field (opt.MySQL.DataDirectory)
		// TODO: validate this field (opt.MySQL.IndexDirectory)
		// TODO: validate this field (opt.MySQL.InsertMethod)
		// TODO: validate this field (opt.MySQL.StorageMedia)
		// TODO: validate this field (opt.MySQL.StatsPersistent)
		// TODO: validate this field (opt.MySQL.StatsAutoRecalc)
		// TODO: validate this field (opt.MySQL.StatsSamplePages)
		// TODO: validate this field (opt.MySQL.Connection)
		// TODO: validate this field (opt.MySQL.Password)
		// TODO: validate this field (opt.MySQL.AutoextendSize)
		// TODO: validate this field (opt.MySQL.Union)
		// TODO: validate this field (opt.MySQL.SecondaryEngine)
		// TODO: validate this field (opt.MySQL.TableChecksum)
		// TODO: validate this field (opt.MySQL.EngineAttribute)
		// TODO: validate this field (opt.MySQL.SecondaryEngineAttribute)
		// TODO: validate this field (opt.MySQL.PageCompressed)
		// TODO: validate this field (opt.MySQL.PageCompressionLevel)
		// TODO: validate this field (opt.MySQL.IetfQuotes)
		// TODO: validate this field (opt.MySQL.Nodegroup)
	}

	if opt.TiDB != nil {
		// TODO: validate this field (opt.TiDB.AutoIDCache)
		// TODO: validate this field (opt.TiDB.AutoRandomBase)
		// TODO: validate this field (opt.TiDB.ShardRowID)
		// TODO: validate this field (opt.TiDB.PreSplitRegion)
		// TODO: validate this field (opt.TiDB.TTL)
		// TODO: validate this field (opt.TiDB.TTLEnable)
		// TODO: validate this field (opt.TiDB.TTLJobInterval)
		// TODO: validate this field (opt.TiDB.Affinity)
		// TODO: validate this field (opt.TiDB.PlacementPolicy)
		// TODO: validate this field (opt.TiDB.StatsBuckets)
		// TODO: validate this field (opt.TiDB.StatsTopN)
		// TODO: validate this field (opt.TiDB.StatsColsChoice)
		// TODO: validate this field (opt.TiDB.StatsColList)
		// TODO: validate this field (opt.TiDB.StatsSampleRate)
		// TODO: validate this field (opt.TiDB.Sequence)
	}

	if opt.PostgreSQL != nil {
		// TODO: validate this field (opt.PostgreSQL.Schema)
		// TODO: validate this field (opt.PostgreSQL.Unlogged)
		// TODO: validate this field (opt.PostgreSQL.Fillfactor)
		// TODO: validate this field (opt.PostgreSQL.PartitionBy)
		// TODO: validate this field (opt.PostgreSQL.Inherits)
	}

	if opt.Oracle != nil {
		// TODO: validate this field (opt.Oracle.Organization)
		// TODO: validate this field (opt.Oracle.Logging)
		// TODO: validate this field (opt.Oracle.Pctfree)
		// TODO: validate this field (opt.Oracle.Pctused)
		// TODO: validate this field (opt.Oracle.InitTrans)
		// TODO: validate this field (opt.Oracle.SegmentCreation)
	}

	if opt.SQLServer != nil {
		// TODO: validate this field (opt.SQLServer.FileGroup)
		// TODO: validate this field (opt.SQLServer.DataCompression)
		// TODO: validate this field (opt.SQLServer.MemoryOptimized)
		// TODO: validate this field (opt.SQLServer.SystemVersioning)
		// TODO: validate this field (opt.SQLServer.TextImageOn)
		// TODO: validate this field (opt.SQLServer.LedgerTable)
	}

	if opt.DB2 != nil {
		// TODO: validate this field (opt.DB2.OrganizeBy)
		// TODO: validate this field (opt.DB2.Compress)
		// TODO: validate this field (opt.DB2.DataCapture)
		// TODO: validate this field (opt.DB2.AppendMode)
		// TODO: validate this field (opt.DB2.Volatile)
	}

	if opt.Snowflake != nil {
		// TODO: validate this field (opt.Snowflake.ClusterBy)
		// TODO: validate this field (opt.Snowflake.DataRetentionDays)
		// TODO: validate this field (opt.Snowflake.ChangeTracking)
		// TODO: validate this field (opt.Snowflake.CopyGrants)
		// TODO: validate this field (opt.Snowflake.Transient)
	}

	if opt.SQLite != nil {
		// TODO: validate this field (opt.SQLite.WithoutRowid)
		// TODO: validate this field (opt.SQLite.Strict)
	}

	if opt.MariaDB != nil {
		// TODO: validate this field (opt.MariaDB.PageChecksum)
		// TODO: validate this field (opt.MariaDB.Transactional)
		// TODO: validate this field (opt.MariaDB.EncryptionKeyID)
		// TODO: validate this field (opt.MariaDB.Sequence)
		// TODO: validate this field (opt.MariaDB.WithSystemVersioning)
	}

	return nil
}

// validatePKConflict ensures a table doesn't define primary keys both at the
// column level (primary_key = true) and in the constraints section. This check
// MUST run before synthesizeConstraints because synthesis merges column-level
// PKs into constraint-level, making the conflict undetectable.
func validatePKConflict(table *Table) error {
	hasColumnPK := false
	for _, col := range table.Columns {
		if col.PrimaryKey {
			hasColumnPK = true
			break
		}
	}
	constraintPKCount := 0
	for _, con := range table.Constraints {
		if con.Type == ConstraintPrimaryKey {
			constraintPKCount++
		}
	}
	if constraintPKCount > 1 {
		return errors.New(
			"multiple PRIMARY KEY constraints declared; a table can have at most one primary key",
		)
	}
	if hasColumnPK && constraintPKCount > 0 {
		return errors.New(
			"primary key declared on both column(s) and in constraints section; " +
				"use column-level primary_key for single-column PKs or a constraint for composite PKs, not both",
		)
	}
	return nil
}

// validateTimestamps checks that the created and updated timestamp columns
// resolve to distinct names and follow naming rules.
func validateTimestamps(table *Table) error {
	if table.Timestamps == nil || !table.Timestamps.Enabled {
		return nil
	}
	createdCol := "created_at"
	updatedCol := "updated_at"
	if table.Timestamps.CreatedColumn != "" {
		if err := validateName(table.Timestamps.CreatedColumn, "timestamp created_column", nil, nil, false); err != nil {
			return err
		}
		createdCol = table.Timestamps.CreatedColumn
	}
	if table.Timestamps.UpdatedColumn != "" {
		if err := validateName(table.Timestamps.UpdatedColumn, "timestamp updated_column", nil, nil, false); err != nil {
			return err
		}
		updatedCol = table.Timestamps.UpdatedColumn
	}
	if createdCol == updatedCol {
		return fmt.Errorf("timestamps created_column and updated_column resolve to the same name %q", createdCol)
	}
	return nil
}
