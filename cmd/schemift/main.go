package main

import (
	"fmt"
	"os"
	"strings"

	"schemift/internal/parser"

	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "schemift",
		Short: "Database migration tool",
	}

	parseCmd := &cobra.Command{
		Use:   "parse <schema.sql>",
		Short: "Parse and display schema",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("failed to read file: %w", err)
			}

			p := parser.NewSQLParser()
			db, err := p.ParseSchema(string(data))
			if err != nil {
				return fmt.Errorf("parse error: %w", err)
			}

			fmt.Printf("Tables found: %d\n", len(db.Tables))
			for _, t := range db.Tables {
				fmt.Printf("- %s (%d columns)\n", t.Name, len(t.Columns))
				for _, c := range t.Columns {
					fmt.Printf("  - %s: %s\n", c.Name, c.TypeRaw)
				}
			}
			return nil
		},
	}

	diffCmd := &cobra.Command{
		Use:   "diff <old.sql> <new.sql>",
		Short: "Compare two schemas",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			oldData, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("failed to read old schema: %w", err)
			}
			newData, err := os.ReadFile(args[1])
			if err != nil {
				return fmt.Errorf("failed to read new schema: %w", err)
			}

			p := parser.NewSQLParser()
			oldDB, err := p.ParseSchema(string(oldData))
			if err != nil {
				return fmt.Errorf("parse old schema error: %w", err)
			}
			newDB, err := p.ParseSchema(string(newData))
			if err != nil {
				return fmt.Errorf("parse new schema error: %w", err)
			}

			// TODO: call core.Diff(oldDB, newDB)
			fmt.Printf("Old DB: %d tables\n", len(oldDB.Tables))
			fmt.Printf("New DB: %d tables\n", len(newDB.Tables))
			return nil
		},
	}

	var fromDialect string
	var toDialect string
	var outFile string

	migrateCmd := &cobra.Command{
		Use:   "migrate <old.sql> <new.sql>",
		Short: "Migrate schema from old dump to new dump",
		Long: `Migrate generates the necessary SQL statements to transition a database schema 
from an old state (old.sql) to a new state (new.sql).
You can specify the source and target database dialects using the --from and --to flags.`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			oldPath := args[0]
			newPath := args[1]

			fmt.Printf("Migrating from %s (%s) to %s (%s)\n", oldPath, fromDialect, newPath, toDialect)

			supported := map[string]bool{"mysql": true, "psql": true, "sqlite": true}
			if !supported[strings.ToLower(fromDialect)] {
				return fmt.Errorf("unsupported source dialect: %s", fromDialect)
			}
			if !supported[strings.ToLower(toDialect)] {
				return fmt.Errorf("unsupported target dialect: %s", toDialect)
			}

			oldData, err := os.ReadFile(oldPath)
			if err != nil {
				return fmt.Errorf("failed to read old schema: %w", err)
			}
			newData, err := os.ReadFile(newPath)
			if err != nil {
				return fmt.Errorf("failed to read new schema: %w", err)
			}

			// TODO: Use appropriate parser based on fromDialect
			p := parser.NewSQLParser()
			oldDB, err := p.ParseSchema(string(oldData))
			if err != nil {
				return fmt.Errorf("failed to parse old schema: %w", err)
			}
			newDB, err := p.ParseSchema(string(newData))
			if err != nil {
				return fmt.Errorf("failed to parse new schema: %w", err)
			}

			// TODO: diff := core.Diff(oldDB, newDB)
			fmt.Printf("Detected changes between schemas (old: %d tables, new: %d tables)\n",
				len(oldDB.Tables), len(newDB.Tables))

			// TODO: migration := migration.Generate(diff, toDialect)

			if outFile != "" {
				// TODO: os.WriteFile(outFile, []byte(migration), 0644)
			} else {
				// os.WriteFile('schemiftMigration.sql', []byte(migration), 0644)
			}

			return nil
		},
	}

	migrateCmd.Flags().StringVarP(&fromDialect, "from", "f", "", "Source database dialect (e.g., mysql, postgres, sqlite)")
	migrateCmd.Flags().StringVarP(&toDialect, "to", "t", "", "Target database dialect (e.g., mysql, postgres, sqlite)")
	migrateCmd.Flags().StringVarP(&outFile, "output", "o", "", "Output file for the generated migration SQL")

	//_ = migrateCmd.MarkFlagRequired("from")
	//_ = migrateCmd.MarkFlagRequired("to")

	rootCmd.AddCommand(parseCmd)
	rootCmd.AddCommand(diffCmd)
	rootCmd.AddCommand(migrateCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
