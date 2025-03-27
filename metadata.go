package main

import "fmt"

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

func (m *Metadata) print() {
	fmt.Printf("%s\n", m.Database)
	for i := 0; i < len(m.Tables); i++ {
		m.Tables[i].print()
	}
}
