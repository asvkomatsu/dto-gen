package main

import (
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
	Funcs   []PythonFunc
}

func (s *PythonSourceFile) addImport(impt PythonImport) {
	s.Imports = append(s.Imports, impt)
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

func generatePythonDbConnector(connInfo *ConnectionInfo, folder string) error {
	fmt.Println("    generating database connector...")

	pythonSource := PythonSourceFile{
		Name:    "db_connector",
		Imports: make([]PythonImport, 0),
		Funcs:   make([]PythonFunc, 0),
	}

	// add imports needed to talk to postgresql
	pythonSource.addImport(PythonImport{Library: "psycopg2", Classes: []string{}})
	pythonSource.addImport(PythonImport{Library: "psycopg2", Classes: []string{"sql"}})
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
	connectFunc.addStatement("        'dbname': '" + connInfo.Database + "'")
	connectFunc.addStatement("        'user': '" + connInfo.Username + "'")
	connectFunc.addStatement("        'password': '" + connInfo.Password + "'")
	connectFunc.addStatement("        'host': '" + connInfo.Host + "'")
	connectFunc.addStatement(fmt.Sprintf("        'port': %d", connInfo.Port))
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

func writePython(connInfo *ConnectionInfo, folder string, metadata *Metadata, customQueries []CustomQuery) error {
	fmt.Println("Generating DTO files on " + folder)

	// remove existing .py files on target directory
	err := removeExistingPythonFiles(folder)
	if err != nil {
		return err
	}

	fmt.Println("Generating new DTO files...")

	// generate db connector
	err = generatePythonDbConnector(connInfo, folder)
	if err != nil {
		return err
	}

	return nil
}
