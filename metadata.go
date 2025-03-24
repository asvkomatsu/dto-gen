package main

import "fmt"

type Column struct {
	Name         string
	Datatype     string
	Nullable     bool
	DefaultValue *string
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
	fmt.Printf("    %s %s", c.Name, c.Datatype)
	if c.Nullable {
		fmt.Print(" NULL ")
	} else {
		fmt.Print(" NOT NULL")
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
