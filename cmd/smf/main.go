// Package main contains the cli implementation of the tool. It uses cobra
// package for cli tool implementation.
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"smf/internal/diff"
	"smf/internal/output"
	"smf/internal/parser/toml"
)

type diffFlags struct {
	outFile       string
	format        string
	detectRenames bool
}

func main() {
	rootCmd := &cobra.Command{
		Use:   "smf",
		Short: "Schema migration framework – TOML-first database schema tool",
	}

	rootCmd.AddCommand(diffCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func diffCmd() *cobra.Command {
	flags := &diffFlags{}
	cmd := &cobra.Command{
		Use:   "diff <old.toml> <new.toml>",
		Short: "Compare two TOML schemas",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			return runDiff(args[0], args[1], flags)
		},
	}

	cmd.Flags().StringVarP(&flags.outFile, "output", "o", "", "Output file for the diff")
	cmd.Flags().StringVarP(&flags.format, "format", "f", "", "Output format: json or summary")
	cmd.Flags().BoolVarP(&flags.detectRenames, "detect-renames", "r", true, "Enable heuristic column rename detection")

	return cmd
}

func runDiff(oldPath, newPath string, flags *diffFlags) error {
	p := toml.NewParser()

	oldDB, err := p.ParseFile(oldPath)
	if err != nil {
		return fmt.Errorf("failed to parse old schema: %w", err)
	}
	newDB, err := p.ParseFile(newPath)
	if err != nil {
		return fmt.Errorf("failed to parse new schema: %w", err)
	}

	schemaDiff := diff.Diff(oldDB, newDB, diff.Options{DetectColumnRenames: flags.detectRenames})
	formatter, err := output.NewFormatter(flags.format)
	if err != nil {
		return err
	}

	formattedDiff, err := formatter.FormatDiff(schemaDiff)
	if err != nil {
		return fmt.Errorf("failed to format output: %w", err)
	}

	return writeOutput(formattedDiff, flags.outFile, flags.format)
}

func printInfo(format string, msg string) {
	if strings.EqualFold(strings.TrimSpace(format), string(output.FormatJSON)) {
		_, _ = fmt.Fprintln(os.Stderr, msg)
		return
	}
	fmt.Println(msg)
}

func writeOutput(content, outFile, format string) error {
	if outFile == "" {
		fmt.Print(content)
		return nil
	}

	if err := os.WriteFile(outFile, []byte(content), 0o644); err != nil {
		return fmt.Errorf("failed to write output: %w", err)
	}

	printInfo(format, fmt.Sprintf("Output saved to %s", outFile))
	return nil
}
