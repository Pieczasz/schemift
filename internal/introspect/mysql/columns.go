package mysql

import (
	"fmt"

	"smf/internal/core"
)

// parseColumn parses a single column definition from a CREATE TABLE body item.
//
// Example input: "`id` bigint unsigned NOT NULL AUTO_INCREMENT".
func parseColumn(_ core.Dialect, item string) (*core.Column, error) {
	// TODO: implement full column parsing (name, type, nullability, default, etc.)
	_ = item
	return nil, fmt.Errorf("parseColumn not yet implemented")
}
