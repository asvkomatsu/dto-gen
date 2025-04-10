package metadata

import (
	"fmt"
	"strings"
)

type ForeignKeyTarget struct {
	Schema string
	Table  string
	Column string
}

type Column struct {
	Ordinal         int
	Name            string
	Datatype        string
	Nullable        bool
	DefaultValue    *string
	IsPrimaryKey    bool
	IsAutoIncrement bool
	FkTarget        *ForeignKeyTarget
}

type Table struct {
	Schema  string
	Name    string
	Columns []Column
}

type Metadata struct {
	Database string
	Tables   []Table
}

func (c *Column) print() {
	fmt.Printf("    [%d] %s %s", c.Ordinal, c.Name, c.Datatype)
	if c.Nullable {
		fmt.Print(" NULL ")
	} else {
		fmt.Print(" NOT NULL")
	}
	if c.IsPrimaryKey {
		fmt.Printf(" PRIMARY KEY")
	}
	if c.IsAutoIncrement {
		fmt.Printf(" AUTOINCREMENT")
	}
	if c.FkTarget != nil {
		fmt.Printf(" FOREIGN KEY (%s.%s->%s)", c.FkTarget.Schema, c.FkTarget.Table, c.FkTarget.Column)
	}
	if c.DefaultValue != nil {
		fmt.Printf(" DEFAULT %s", *c.DefaultValue)
	}

	fmt.Printf("\n")
}

func (t *Table) print() {
	fmt.Printf("  %s.%s\n", t.Schema, t.Name)
	for i := 0; i < len(t.Columns); i++ {
		t.Columns[i].print()
	}
}

func (t *Table) SearchColumnByName(name string) *Column {
	for i := range t.Columns {
		if t.Columns[i].Name == name {
			return &t.Columns[i]
		}
	}
	return nil
}

func (m *Metadata) print() {
	fmt.Printf("%s\n", m.Database)
	for i := 0; i < len(m.Tables); i++ {
		m.Tables[i].print()
	}
}

func (m *Metadata) SearchTableByName(name string) *Table {
	for i := range m.Tables {
		if m.Tables[i].Name == name {
			return &m.Tables[i]
		}
	}
	return nil
}

func ToPascalCase(input string) string {
	parts := strings.Split(input, "_")

	var result []string
	for _, part := range parts {
		if len(part) == 0 {
			continue // Skip empty parts caused by consecutive underscores
		}
		// Capitalize the first character and append the rest of the string
		result = append(result, strings.ToUpper(string(part[0]))+part[1:])
	}

	// Join the parts back together
	return strings.Join(result, "")
}

func ToCamelCase(input string) string {
	parts := strings.Split(input, "_")

	var result []string
	for i, part := range parts {
		if len(part) == 0 {
			continue // Skip empty parts caused by consecutive underscores
		}
		if i > 0 {
			// Capitalize the first character and append the rest of the string
			result = append(result, strings.ToUpper(string(part[0]))+part[1:])
		} else {
			result = append(result, part)
		}
	}

	return strings.Join(result, "")
}

func ContainsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}
