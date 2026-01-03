package core

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

type SchemaDiff struct {
	AddedTables    []*Table
	RemovedTables  []*Table
	ModifiedTables []*TableDiff
}

type TableDiff struct {
	Name                string
	AddedColumns        []*Column
	RemovedColumns      []*Column
	ModifiedColumns     []*ColumnChange
	AddedConstraints    []*Constraint
	RemovedConstraints  []*Constraint
	ModifiedConstraints []*ConstraintChange
	AddedIndexes        []*Index
	RemovedIndexes      []*Index
	ModifiedIndexes     []*IndexChange
	ModifiedOptions     []*TableOptionChange
}

type ColumnChange struct {
	Name    string
	Old     *Column
	New     *Column
	Changes []*FieldChange
}

type ConstraintChange struct {
	Name    string
	Old     *Constraint
	New     *Constraint
	Changes []*FieldChange
}

type IndexChange struct {
	Name    string
	Old     *Index
	New     *Index
	Changes []*FieldChange
}

type FieldChange struct {
	Field string
	Old   string
	New   string
}

type TableOptionChange struct {
	Name string
	Old  string
	New  string
}

func Diff(oldDB, newDB *Database) *SchemaDiff {
	d := &SchemaDiff{}

	oldTables := map[string]*Table{}
	newTables := map[string]*Table{}

	for _, t := range oldDB.Tables {
		oldTables[strings.ToLower(t.Name)] = t
	}
	for _, t := range newDB.Tables {
		newTables[strings.ToLower(t.Name)] = t
	}

	for name, nt := range newTables {
		ot, ok := oldTables[name]
		if !ok {
			d.AddedTables = append(d.AddedTables, nt)
			continue
		}

		td := compareTable(ot, nt)
		if td != nil {
			d.ModifiedTables = append(d.ModifiedTables, td)
		}
	}

	for name, ot := range oldTables {
		if _, ok := newTables[name]; !ok {
			d.RemovedTables = append(d.RemovedTables, ot)
		}
	}

	sort.Slice(d.AddedTables, func(i, j int) bool {
		return strings.ToLower(d.AddedTables[i].Name) < strings.ToLower(d.AddedTables[j].Name)
	})
	sort.Slice(d.RemovedTables, func(i, j int) bool {
		return strings.ToLower(d.RemovedTables[i].Name) < strings.ToLower(d.RemovedTables[j].Name)
	})
	sort.Slice(d.ModifiedTables, func(i, j int) bool {
		return strings.ToLower(d.ModifiedTables[i].Name) < strings.ToLower(d.ModifiedTables[j].Name)
	})

	return d
}

func compareTable(oldT, newT *Table) *TableDiff {
	td := &TableDiff{Name: newT.Name}

	oldCols := map[string]*Column{}
	newCols := map[string]*Column{}
	for _, c := range oldT.Columns {
		oldCols[strings.ToLower(c.Name)] = c
	}
	for _, c := range newT.Columns {
		newCols[strings.ToLower(c.Name)] = c
	}

	for name, nc := range newCols {
		if oc, ok := oldCols[name]; !ok {
			td.AddedColumns = append(td.AddedColumns, nc)
		} else if !equalColumn(oc, nc) {
			td.ModifiedColumns = append(td.ModifiedColumns, &ColumnChange{Name: nc.Name, Old: oc, New: nc, Changes: columnFieldChanges(oc, nc)})
		}
	}
	for name, oc := range oldCols {
		if _, ok := newCols[name]; !ok {
			td.RemovedColumns = append(td.RemovedColumns, oc)
		}
	}

	oldCons := map[string]*Constraint{}
	newCons := map[string]*Constraint{}
	for _, c := range oldT.Constraints {
		oldCons[constraintKey(c)] = c
	}
	for _, c := range newT.Constraints {
		newCons[constraintKey(c)] = c
	}
	for name, nc := range newCons {
		oc, ok := oldCons[name]
		if !ok {
			td.AddedConstraints = append(td.AddedConstraints, nc)
			continue
		}
		if !equalConstraint(oc, nc) {
			td.ModifiedConstraints = append(td.ModifiedConstraints, &ConstraintChange{Name: nc.Name, Old: oc, New: nc, Changes: constraintFieldChanges(oc, nc)})
		}
	}
	for name, oc := range oldCons {
		if _, ok := newCons[name]; !ok {
			td.RemovedConstraints = append(td.RemovedConstraints, oc)
		}
	}

	oldIdx := map[string]*Index{}
	newIdx := map[string]*Index{}
	for _, i := range oldT.Indexes {
		oldIdx[indexKey(i)] = i
	}
	for _, i := range newT.Indexes {
		newIdx[indexKey(i)] = i
	}
	for name, ni := range newIdx {
		oi, ok := oldIdx[name]
		if !ok {
			td.AddedIndexes = append(td.AddedIndexes, ni)
			continue
		}
		if !equalIndex(oi, ni) {
			td.ModifiedIndexes = append(td.ModifiedIndexes, &IndexChange{Name: ni.Name, Old: oi, New: ni, Changes: indexFieldChanges(oi, ni)})
		}
	}
	for name, oi := range oldIdx {
		if _, ok := newIdx[name]; !ok {
			td.RemovedIndexes = append(td.RemovedIndexes, oi)
		}
	}

	// table options
	oldOpt := tableOptionMap(oldT)
	newOpt := tableOptionMap(newT)
	for _, k := range unionKeys(oldOpt, newOpt) {
		ov, nv := oldOpt[k], newOpt[k]
		if ov == nv {
			continue
		}
		td.ModifiedOptions = append(td.ModifiedOptions, &TableOptionChange{Name: k, Old: ov, New: nv})
	}

	if len(td.AddedColumns) == 0 && len(td.RemovedColumns) == 0 && len(td.ModifiedColumns) == 0 && len(td.AddedConstraints) == 0 && len(td.RemovedConstraints) == 0 && len(td.ModifiedConstraints) == 0 && len(td.AddedIndexes) == 0 && len(td.RemovedIndexes) == 0 && len(td.ModifiedIndexes) == 0 && len(td.ModifiedOptions) == 0 {
		return nil
	}

	sort.Slice(td.AddedColumns, func(i, j int) bool {
		return strings.ToLower(td.AddedColumns[i].Name) < strings.ToLower(td.AddedColumns[j].Name)
	})
	sort.Slice(td.RemovedColumns, func(i, j int) bool {
		return strings.ToLower(td.RemovedColumns[i].Name) < strings.ToLower(td.RemovedColumns[j].Name)
	})
	sort.Slice(td.ModifiedColumns, func(i, j int) bool {
		return strings.ToLower(td.ModifiedColumns[i].Name) < strings.ToLower(td.ModifiedColumns[j].Name)
	})
	sort.Slice(td.AddedConstraints, func(i, j int) bool {
		return strings.ToLower(td.AddedConstraints[i].Name) < strings.ToLower(td.AddedConstraints[j].Name)
	})
	sort.Slice(td.RemovedConstraints, func(i, j int) bool {
		return strings.ToLower(td.RemovedConstraints[i].Name) < strings.ToLower(td.RemovedConstraints[j].Name)
	})
	sort.Slice(td.ModifiedConstraints, func(i, j int) bool {
		return strings.ToLower(td.ModifiedConstraints[i].Name) < strings.ToLower(td.ModifiedConstraints[j].Name)
	})
	sort.Slice(td.AddedIndexes, func(i, j int) bool {
		return strings.ToLower(td.AddedIndexes[i].Name) < strings.ToLower(td.AddedIndexes[j].Name)
	})
	sort.Slice(td.RemovedIndexes, func(i, j int) bool {
		return strings.ToLower(td.RemovedIndexes[i].Name) < strings.ToLower(td.RemovedIndexes[j].Name)
	})
	sort.Slice(td.ModifiedIndexes, func(i, j int) bool {
		return strings.ToLower(td.ModifiedIndexes[i].Name) < strings.ToLower(td.ModifiedIndexes[j].Name)
	})
	sort.Slice(td.ModifiedOptions, func(i, j int) bool {
		return strings.ToLower(td.ModifiedOptions[i].Name) < strings.ToLower(td.ModifiedOptions[j].Name)
	})

	return td
}

func equalColumn(a, b *Column) bool {
	if strings.ToLower(a.TypeRaw) != strings.ToLower(b.TypeRaw) {
		return false
	}
	if a.Nullable != b.Nullable {
		return false
	}
	if a.PrimaryKey != b.PrimaryKey {
		return false
	}
	if a.AutoIncrement != b.AutoIncrement {
		return false
	}
	if strings.ToLower(a.Charset) != strings.ToLower(b.Charset) {
		return false
	}
	if strings.ToLower(a.Collate) != strings.ToLower(b.Collate) {
		return false
	}
	if a.Comment != b.Comment {
		return false
	}
	var av, bv string
	if a.DefaultValue != nil {
		av = *a.DefaultValue
	}
	if b.DefaultValue != nil {
		bv = *b.DefaultValue
	}
	if av != bv {
		return false
	}
	var au, bu string
	if a.OnUpdate != nil {
		au = *a.OnUpdate
	}
	if b.OnUpdate != nil {
		bu = *b.OnUpdate
	}
	if au != bu {
		return false
	}
	if a.IsGenerated != b.IsGenerated {
		return false
	}
	if strings.TrimSpace(a.GenerationExpression) != strings.TrimSpace(b.GenerationExpression) {
		return false
	}
	if strings.ToLower(strings.TrimSpace(a.GenerationStorage)) != strings.ToLower(strings.TrimSpace(b.GenerationStorage)) {
		return false
	}
	return true
}

func equalConstraint(a, b *Constraint) bool {
	if a.Type != b.Type {
		return false
	}
	if !equalStringSliceCI(a.Columns, b.Columns) {
		return false
	}
	if !strings.EqualFold(a.ReferencedTable, b.ReferencedTable) {
		return false
	}
	if !equalStringSliceCI(a.ReferencedColumns, b.ReferencedColumns) {
		return false
	}
	if !strings.EqualFold(a.OnDelete, b.OnDelete) {
		return false
	}
	if !strings.EqualFold(a.OnUpdate, b.OnUpdate) {
		return false
	}
	if strings.TrimSpace(a.CheckExpression) != strings.TrimSpace(b.CheckExpression) {
		return false
	}
	return true
}

func equalIndex(a, b *Index) bool {
	if a.Unique != b.Unique {
		return false
	}
	if !strings.EqualFold(a.Type, b.Type) {
		return false
	}
	if !equalStringSliceCI(a.Columns, b.Columns) {
		return false
	}
	return true
}

func equalStringSliceCI(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !strings.EqualFold(a[i], b[i]) {
			return false
		}
	}
	return true
}

func constraintKey(c *Constraint) string {
	name := strings.ToLower(strings.TrimSpace(c.Name))
	if name != "" {
		return name
	}
	return strings.ToLower(string(c.Type)) + ":" + strings.ToLower(strings.Join(c.Columns, ","))
}

func indexKey(i *Index) string {
	name := strings.ToLower(strings.TrimSpace(i.Name))
	if name != "" {
		return name
	}
	uniq := "0"
	if i.Unique {
		uniq = "1"
	}
	return "idx:" + uniq + ":" + strings.ToLower(i.Type) + ":" + strings.ToLower(strings.Join(i.Columns, ","))
}

func tableOptionMap(t *Table) map[string]string {
	return map[string]string{
		"AUTOEXTEND_SIZE":    strings.TrimSpace(t.AutoextendSize),
		"AUTO_INCREMENT":     u64(t.AutoIncrement),
		"AVG_ROW_LENGTH":     u64(t.AvgRowLength),
		"CHARSET":            strings.TrimSpace(t.Charset),
		"CHECKSUM":           u64(t.Checksum),
		"COLLATE":            strings.TrimSpace(t.Collate),
		"COMMENT":            strings.TrimSpace(t.Comment),
		"COMPRESSION":        strings.TrimSpace(t.Compression),
		"CONNECTION":         strings.TrimSpace(t.Connection),
		"DATA DIRECTORY":     strings.TrimSpace(t.DataDirectory),
		"DELAY_KEY_WRITE":    u64(t.DelayKeyWrite),
		"ENCRYPTION":         strings.TrimSpace(t.Encryption),
		"ENGINE":             strings.TrimSpace(t.Engine),
		"INDEX DIRECTORY":    strings.TrimSpace(t.IndexDirectory),
		"INSERT_METHOD":      strings.TrimSpace(t.InsertMethod),
		"KEY_BLOCK_SIZE":     u64(t.KeyBlockSize),
		"MAX_ROWS":           u64(t.MaxRows),
		"MIN_ROWS":           u64(t.MinRows),
		"PAGE_CHECKSUM":      u64(t.PageChecksum),
		"PASSWORD":           strings.TrimSpace(t.Password),
		"ROW_FORMAT":         strings.TrimSpace(t.RowFormat),
		"STATS_AUTO_RECALC":  strings.TrimSpace(t.StatsAutoRecalc),
		"STATS_SAMPLE_PAGES": strings.TrimSpace(t.StatsSamplePages),
		"TABLESPACE":         strings.TrimSpace(t.Tablespace),
		"TRANSACTIONAL":      u64(t.Transactional),
	}
}

func unionKeys(a, b map[string]string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		seen[k] = struct{}{}
	}
	for k := range b {
		seen[k] = struct{}{}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return strings.ToLower(keys[i]) < strings.ToLower(keys[j]) })
	return keys
}

func u64(v uint64) string {
	return strconv.FormatUint(v, 10)
}

func columnFieldChanges(oldC, newC *Column) []*FieldChange {
	var changes []*FieldChange
	add := func(field, oldV, newV string) {
		if oldV == newV {
			return
		}
		changes = append(changes, &FieldChange{Field: field, Old: oldV, New: newV})
	}

	if !strings.EqualFold(oldC.TypeRaw, newC.TypeRaw) {
		add("type", oldC.TypeRaw, newC.TypeRaw)
	}
	add("nullable", strconv.FormatBool(oldC.Nullable), strconv.FormatBool(newC.Nullable))
	add("primary_key", strconv.FormatBool(oldC.PrimaryKey), strconv.FormatBool(newC.PrimaryKey))
	add("auto_increment", strconv.FormatBool(oldC.AutoIncrement), strconv.FormatBool(newC.AutoIncrement))
	add("charset", strings.TrimSpace(oldC.Charset), strings.TrimSpace(newC.Charset))
	add("collate", strings.TrimSpace(oldC.Collate), strings.TrimSpace(newC.Collate))
	add("comment", oldC.Comment, newC.Comment)
	add("default", optString(oldC.DefaultValue), optString(newC.DefaultValue))
	add("on_update", optString(oldC.OnUpdate), optString(newC.OnUpdate))
	add("generated", strconv.FormatBool(oldC.IsGenerated), strconv.FormatBool(newC.IsGenerated))
	add("generation_expression", strings.TrimSpace(oldC.GenerationExpression), strings.TrimSpace(newC.GenerationExpression))
	add("generation_storage", strings.TrimSpace(oldC.GenerationStorage), strings.TrimSpace(newC.GenerationStorage))

	return changes
}

func constraintFieldChanges(oldC, newC *Constraint) []*FieldChange {
	var changes []*FieldChange
	add := func(field, oldV, newV string) {
		if oldV == newV {
			return
		}
		changes = append(changes, &FieldChange{Field: field, Old: oldV, New: newV})
	}

	add("type", string(oldC.Type), string(newC.Type))
	add("columns", fmt.Sprintf("%v", oldC.Columns), fmt.Sprintf("%v", newC.Columns))
	add("referenced_table", oldC.ReferencedTable, newC.ReferencedTable)
	add("referenced_columns", fmt.Sprintf("%v", oldC.ReferencedColumns), fmt.Sprintf("%v", newC.ReferencedColumns))
	add("on_delete", strings.TrimSpace(oldC.OnDelete), strings.TrimSpace(newC.OnDelete))
	add("on_update", strings.TrimSpace(oldC.OnUpdate), strings.TrimSpace(newC.OnUpdate))
	add("check_expression", strings.TrimSpace(oldC.CheckExpression), strings.TrimSpace(newC.CheckExpression))

	return changes
}

func indexFieldChanges(oldI, newI *Index) []*FieldChange {
	var changes []*FieldChange
	add := func(field, oldV, newV string) {
		if oldV == newV {
			return
		}
		changes = append(changes, &FieldChange{Field: field, Old: oldV, New: newV})
	}

	add("unique", strconv.FormatBool(oldI.Unique), strconv.FormatBool(newI.Unique))
	add("type", strings.TrimSpace(oldI.Type), strings.TrimSpace(newI.Type))
	add("columns", fmt.Sprintf("%v", oldI.Columns), fmt.Sprintf("%v", newI.Columns))

	return changes
}

func optString(v *string) string {
	if v == nil {
		return "<nil>"
	}
	return *v
}

func (d *SchemaDiff) String() string {
	var sb strings.Builder
	if len(d.AddedTables) == 0 && len(d.RemovedTables) == 0 && len(d.ModifiedTables) == 0 {
		return "No differences detected."
	}

	sb.WriteString("Schema differences:\n")

	if len(d.AddedTables) > 0 {
		sb.WriteString("\nAdded tables:\n")
		for _, t := range d.AddedTables {
			sb.WriteString(fmt.Sprintf("  - %s\n", t.Name))
		}
	}
	if len(d.RemovedTables) > 0 {
		sb.WriteString("\nRemoved tables:\n")
		for _, t := range d.RemovedTables {
			sb.WriteString(fmt.Sprintf("  - %s\n", t.Name))
		}
	}
	if len(d.ModifiedTables) > 0 {
		sb.WriteString("\nModified tables:\n")
		for _, mt := range d.ModifiedTables {
			sb.WriteString(fmt.Sprintf("\n  - %s\n", mt.Name))
			if len(mt.ModifiedOptions) > 0 {
				sb.WriteString("    Options changed:\n")
				for _, oc := range mt.ModifiedOptions {
					sb.WriteString(fmt.Sprintf("      - %s: %q -> %q\n", oc.Name, oc.Old, oc.New))
				}
			}
			if len(mt.AddedColumns) > 0 {
				sb.WriteString("    Added columns:\n")
				for _, c := range mt.AddedColumns {
					sb.WriteString(fmt.Sprintf("      - %s: %s\n", c.Name, c.TypeRaw))
				}
			}
			if len(mt.RemovedColumns) > 0 {
				sb.WriteString("    Removed columns:\n")
				for _, c := range mt.RemovedColumns {
					sb.WriteString(fmt.Sprintf("      - %s: %s\n", c.Name, c.TypeRaw))
				}
			}
			if len(mt.ModifiedColumns) > 0 {
				sb.WriteString("    Modified columns:\n")
				for _, ch := range mt.ModifiedColumns {
					sb.WriteString(fmt.Sprintf("      - %s:\n", ch.Name))
					if len(ch.Changes) > 0 {
						for _, fc := range ch.Changes {
							sb.WriteString(fmt.Sprintf("        - %s: %q -> %q\n", fc.Field, fc.Old, fc.New))
						}
					} else {
						sb.WriteString(fmt.Sprintf("        - old: %s (nullable=%v)\n", ch.Old.TypeRaw, ch.Old.Nullable))
						sb.WriteString(fmt.Sprintf("        - new: %s (nullable=%v)\n", ch.New.TypeRaw, ch.New.Nullable))
					}
				}
			}
			if len(mt.AddedConstraints) > 0 {
				sb.WriteString("    Added constraints:\n")
				for _, c := range mt.AddedConstraints {
					sb.WriteString(fmt.Sprintf("      - %s (%s)\n", c.Name, c.Type))
				}
			}
			if len(mt.RemovedConstraints) > 0 {
				sb.WriteString("    Removed constraints:\n")
				for _, c := range mt.RemovedConstraints {
					sb.WriteString(fmt.Sprintf("      - %s (%s)\n", c.Name, c.Type))
				}
			}
			if len(mt.ModifiedConstraints) > 0 {
				sb.WriteString("    Modified constraints:\n")
				for _, ch := range mt.ModifiedConstraints {
					name := ch.Name
					if strings.TrimSpace(name) == "" {
						name = string(ch.New.Type)
					}
					sb.WriteString(fmt.Sprintf("      - %s:\n", name))
					if len(ch.Changes) > 0 {
						for _, fc := range ch.Changes {
							sb.WriteString(fmt.Sprintf("        - %s: %q -> %q\n", fc.Field, fc.Old, fc.New))
						}
					}
				}
			}
			if len(mt.AddedIndexes) > 0 {
				sb.WriteString("    Added indexes:\n")
				for _, idx := range mt.AddedIndexes {
					sb.WriteString(fmt.Sprintf("      - %s (%v)\n", idx.Name, idx.Columns))
				}
			}
			if len(mt.RemovedIndexes) > 0 {
				sb.WriteString("    Removed indexes:\n")
				for _, idx := range mt.RemovedIndexes {
					sb.WriteString(fmt.Sprintf("      - %s (%v)\n", idx.Name, idx.Columns))
				}
			}
			if len(mt.ModifiedIndexes) > 0 {
				sb.WriteString("    Modified indexes:\n")
				for _, ch := range mt.ModifiedIndexes {
					name := ch.Name
					if strings.TrimSpace(name) == "" {
						name = "(unnamed index)"
					}
					sb.WriteString(fmt.Sprintf("      - %s:\n", name))
					if len(ch.Changes) > 0 {
						for _, fc := range ch.Changes {
							sb.WriteString(fmt.Sprintf("        - %s: %q -> %q\n", fc.Field, fc.Old, fc.New))
						}
					}
				}
			}
		}
	}

	return sb.String()
}

func (d *SchemaDiff) SaveToFile(path string) error {
	return os.WriteFile(path, []byte(d.String()), 0644)
}
