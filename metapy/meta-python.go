package metapy

import (
	"dto-gen/config"
	"dto-gen/metadata"
	"dto-gen/pgsql"
	"fmt"
	"os"
	"path/filepath"
)

// ======================================================================================
//     Metaprogramming
// ======================================================================================

type PythonSourceFile struct {
	Name    string
	Imports []PythonImport
	Classes []PythonClass
	Funcs   []PythonFunc
}

func (s *PythonSourceFile) addImport(impt PythonImport) {
	s.Imports = append(s.Imports, impt)
}

func (s *PythonSourceFile) addClass(cls PythonClass) {
	s.Classes = append(s.Classes, cls)
}

func (s *PythonSourceFile) addFunc(f PythonFunc) {
	s.Funcs = append(s.Funcs, f)
}

type PythonImport struct {
	Library string
	Classes []string
}

func (i *PythonImport) toString() string {
	if len(i.Classes) > 0 {
		classesList := ""
		for idx := range i.Classes {
			if idx > 0 {
				classesList += ", " + i.Classes[idx]
			} else {
				classesList += i.Classes[idx]
			}
		}
		return fmt.Sprintf("from %s import %s", i.Library, classesList)
	}
	return fmt.Sprintf("import %s", i.Library)
}

type PythonClass struct {
	Name       string
	Annotation *string
	Fields     []PythonDataClassField
}

func (c *PythonClass) addField(f PythonDataClassField) {
	c.Fields = append(c.Fields, f)
}

type PythonDataClassField struct {
	Name       string
	Type       string
	IsOptional bool
}

type PythonFunc struct {
	Name       string
	Parameters []PythonParameter
	ReturnType string
	Statements []string
}

func (f *PythonFunc) addParameter(p PythonParameter) {
	f.Parameters = append(f.Parameters, p)
}

func (f *PythonFunc) addStatement(s string) {
	f.Statements = append(f.Statements, s)
}

func (f *PythonFunc) toString() string {
	t := fmt.Sprintf("def %s(", f.Name)
	for i := range f.Parameters {
		if i > 0 {
			t += ", " + f.Parameters[i].toString()
		} else {
			t += f.Parameters[i].toString()
		}
	}
	t += ")"
	if f.ReturnType != "" {
		t += " -> " + f.ReturnType
	}
	t += ":\n"

	for i := range f.Statements {
		t += "    " + f.Statements[i] + "\n"
	}
	return t
}

type PythonParameter struct {
	Name         string
	Type         string
	DefaultValue *string
}

func (p *PythonParameter) toString() string {
	t := fmt.Sprintf("%s: %s", p.Name, p.Type)
	if p.DefaultValue != nil {
		t += fmt.Sprintf(" = %s", *p.DefaultValue)
	}
	return t
}

func writePythonSource(folder string, source PythonSourceFile) error {
	// create file
	text := ""

	// write imports
	for i := range source.Imports {
		text += source.Imports[i].toString() + "\n"
	}
	text += "\n\n"

	// write classes
	for i := range source.Classes {
		class := source.Classes[i]
		if class.Annotation != nil {
			text += "@" + *class.Annotation + "\n"
		}
		text += "class " + class.Name + ":\n"

		for j := range class.Fields {
			field := class.Fields[j]
			text += "    " + field.Name + ": "
			if field.IsOptional {
				text += "Optional[" + field.Type + "]\n"
			} else {
				text += field.Type + "\n"
			}
		}
	}
	text += "\n\n"

	// write funcs
	for i := range source.Funcs {
		text += source.Funcs[i].toString() + "\n"
	}

	// create or truncate the file
	filePath := filepath.Join(folder, source.Name+".py")
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}

	_, err = file.WriteString(text)
	if err != nil {
		return err
	}

	file.Close()

	// TODO: formatSource(filePath)

	return nil
}

// ======================================================================================
//     DTO Generation
// ======================================================================================

func removeExistingPythonFiles(folder string) error {
	fmt.Println("Removing old DTO files...")
	err := filepath.Walk(folder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && filepath.Ext(path) == ".py" {
			fmt.Printf("    removing %s\n", path)
			if err := os.Remove(path); err != nil {
				fmt.Printf("Error removing %s\n", path)
				return fmt.Errorf("error removing %s: %s", path, err)
			}
		}
		return nil
	})

	return err
}

func generateInitPyFile(folder string) error {
	fmt.Println("    generating __init__.py file...")

	pythonSource := PythonSourceFile{
		Name:    "__init__",
		Imports: make([]PythonImport, 0),
		Funcs:   make([]PythonFunc, 0),
	}

	err := writePythonSource(folder, pythonSource)
	if err != nil {
		return err
	}

	return nil
}

func generatePythonDbConnector(connInfo *config.ConnectionInfo, folder string) error {
	fmt.Println("    generating database connector...")

	pythonSource := PythonSourceFile{
		Name:    "db_connector",
		Imports: make([]PythonImport, 0),
		Funcs:   make([]PythonFunc, 0),
	}

	// add imports needed to talk to postgresql
	pythonSource.addImport(PythonImport{Library: "psycopg2", Classes: []string{}})
	pythonSource.addImport(PythonImport{Library: "psycopg2.extensions", Classes: []string{"connection"}})
	pythonSource.addImport(PythonImport{Library: "typing", Classes: []string{"Dict", "Union"}})

	// add connect function
	connectFunc := PythonFunc{
		Name:       "connect",
		Parameters: make([]PythonParameter, 0),
		ReturnType: "connection",
		Statements: make([]string, 0),
	}
	connectFunc.Parameters = append(connectFunc.Parameters, PythonParameter{
		Name:         "db_config",
		Type:         "Dict[str, Union[str, bool]]",
		DefaultValue: nil,
	})
	connectFunc.addStatement("if db_config is None:")
	connectFunc.addStatement("    db_config = {")
	connectFunc.addStatement("        'dbname': '" + connInfo.Database + "',")
	connectFunc.addStatement("        'user': '" + connInfo.Username + "',")
	connectFunc.addStatement("        'password': '" + connInfo.Password + "',")
	connectFunc.addStatement("        'host': '" + connInfo.Host + "',")
	connectFunc.addStatement(fmt.Sprintf("        'port': %d,", connInfo.Port))
	connectFunc.addStatement("    }\n")
	connectFunc.addStatement("try:")
	connectFunc.addStatement("    conn = psycopg2.connect(**db_config)")
	connectFunc.addStatement("    return conn")
	connectFunc.addStatement("except Exception as e:")
	connectFunc.addStatement("    print(f\"Error connecting to the database: {e}\")")
	connectFunc.addStatement("    return None")
	pythonSource.addFunc(connectFunc)

	err := writePythonSource(folder, pythonSource)
	if err != nil {
		return err
	}

	return nil
}

func generatePythonTableDataclass(table *metadata.Table, source *PythonSourceFile) error {
	dataClassAnnotation := "dataclass"
	entity := PythonClass{
		Name:       metadata.ToPascalCase(table.Name),
		Annotation: &dataClassAnnotation,
		Fields:     make([]PythonDataClassField, 0),
	}

	for i := range table.Columns {
		col := table.Columns[i]
		entity.Fields = append(entity.Fields, PythonDataClassField{
			Name:       col.Name,
			Type:       pgsql.PostgreSQLToPythonTypes[col.Datatype],
			IsOptional: col.Nullable,
		})
	}

	source.addClass(entity)
	return nil
}

func generatePythonDTO(folder string, table *metadata.Table) error {
	fmt.Printf("    generating DTO for %s ...\n", table.Name)

	pythonSource := PythonSourceFile{
		Name:    table.Name,
		Imports: make([]PythonImport, 0),
		Funcs:   make([]PythonFunc, 0),
	}

	// add imports needed to talk to postgresql
	pythonSource.addImport(PythonImport{Library: "psycopg2", Classes: []string{}})
	pythonSource.addImport(PythonImport{Library: "psycopg2", Classes: []string{"sql"}})
	pythonSource.addImport(PythonImport{Library: "psycopg2.extensions", Classes: []string{"connection"}})
	pythonSource.addImport(PythonImport{Library: "typing", Classes: []string{"Dict", "Union", "Optional"}})
	pythonSource.addImport(PythonImport{Library: "dataclasses", Classes: []string{"dataclass"}})
	pythonSource.addImport(PythonImport{Library: "datetime", Classes: []string{}})

	// generate table dataclass used throughout the file
	err := generatePythonTableDataclass(table, &pythonSource)
	if err != nil {
		return err
	}

	err = writePythonSource(folder, pythonSource)
	if err != nil {
		return err
	}

	return nil
}

func WritePython(connInfo *config.ConnectionInfo, folder string, metadata *metadata.Metadata, customQueries []config.CustomQuery) error {
	fmt.Println("Generating DTO files on " + folder)

	// remove existing .py files on target directory
	err := removeExistingPythonFiles(folder)
	if err != nil {
		return err
	}

	fmt.Println("Generating new DTO files...")

	// generate db connector
	err = generateInitPyFile(folder)
	if err != nil {
		return err
	}

	// generate db connector
	err = generatePythonDbConnector(connInfo, folder)
	if err != nil {
		return err
	}

	// generate source file for each table
	for i := range metadata.Tables {
		err = generatePythonDTO(folder, &metadata.Tables[i])
		if err != nil {
			return err
		}
	}

	return nil
}
