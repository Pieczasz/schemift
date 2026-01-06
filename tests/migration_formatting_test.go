package tests

import (
	"testing"

	"schemift/core"

	"github.com/stretchr/testify/assert"
)

func TestMigrationString_MultiLineNotesAreCommented(t *testing.T) {
	m := &core.Migration{}
	m.AddNote("line1\nline2")
	m.AddRollbackStatement("ALTER TABLE t ADD COLUMN c INT")

	out := m.String()
	assert.Contains(t, out, "-- NOTES")
	assert.Contains(t, out, "-- - line1")
	assert.Contains(t, out, "-- - line2")
	assert.NotContains(t, out, "\nline2\n")
	assert.Contains(t, out, "-- ROLLBACK SQL")
	assert.Contains(t, out, "-- ALTER TABLE t ADD COLUMN c INT;")

	rb := m.RollbackString()
	assert.Contains(t, rb, "-- schemift rollback")
	assert.Contains(t, rb, "ALTER TABLE t ADD COLUMN c INT;")
}
