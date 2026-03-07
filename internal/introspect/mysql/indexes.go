package mysql

import (
	"fmt"

	"smf/internal/core"
)

// parseIndex parses an inline index declaration from a CREATE TABLE body item.
//
// Handles: KEY, INDEX, FULLTEXT KEY/INDEX, SPATIAL KEY/INDEX.
//
// Example input: "KEY `idx_name` (`name`)"
// Example input: "FULLTEXT INDEX `ft_content` (`content`)".
func parseIndex(_ core.Dialect, item string) (*core.Index, error) {
	// TODO: implement full index parsing.
	_ = item
	return nil, fmt.Errorf("parseIndex not yet implemented")
}
