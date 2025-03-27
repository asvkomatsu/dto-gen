package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

//======================================================================================
//     Metaprogramming
//======================================================================================

type GoSourceFile struct {
	Name    string
	Package string
	Imports []string
	Structs []GoStruct
	Funcs   []GoFuncs
}

func (s *GoSourceFile) addImport(i string) {
	s.Imports = append(s.Imports, i)
}

func (s *GoSourceFile) addStruct(st GoStruct) {
	s.Structs = append(s.Structs, st)
}

func (s *GoSourceFile) addFunc(f GoFuncs) {
	s.Funcs = append(s.Funcs, f)
}

type GoStruct struct {
	Name   string
	Fields []GoStructField
}

func (s *GoStruct) addField(f GoStructField) {
	s.Fields = append(s.Fields, f)
}

type GoStructField struct {
	Name      string
	Type      string
	IsPointer bool
}

type GoFuncs struct {
	Name    string
	Args    []GoFuncArg
	Returns []GoFuncReturn
	Lines   []string
}

func (f *GoFuncs) addArg(arg GoFuncArg) {
	f.Args = append(f.Args, arg)
}

func (f *GoFuncs) addReturn(ret GoFuncReturn) {
	f.Returns = append(f.Returns, ret)
}

func (f *GoFuncs) addLine(line string) {
	f.Lines = append(f.Lines, line)
}

type GoFuncArg struct {
	Name      string
	Type      string
	IsPointer bool
}

type GoFuncReturn struct {
	Type      string
	IsPointer bool
}

func writeSource(folder string, source GoSourceFile) error {
	text := ""
	// add package
	text += "package " + source.Package + "\n\n"

	// add imports
	if len(source.Imports) > 0 {
		text += "import (\n"
		for i := range source.Imports {
			text += "    \"" + source.Imports[i] + "\"\n"
		}
		text += ")\n\n"
	}

	// add stucts
	for i := range source.Structs {
		text += "type " + source.Structs[i].Name + " struct {\n"
		for j := range source.Structs[i].Fields {
			text += "    " + source.Structs[i].Fields[j].Name + " "
			if source.Structs[i].Fields[j].IsPointer {
				text += "*"
			}
			text += source.Structs[i].Fields[j].Type + "\n"
		}
		text += "}\n\n"
	}

	// add funcs
	for i := range source.Funcs {
		f := source.Funcs[i]
		text += "func " + f.Name + "("
		for j := range f.Args {
			text += f.Args[j].Name + " "
			if f.Args[j].IsPointer {
				text += "*"
			}
			text += f.Args[j].Type
			if j < len(f.Args)-1 {
				text += ", "
			}
		}
		text += ") "
		if len(f.Returns) > 1 {
			text += "("
		}
		for j := range f.Returns {
			if f.Returns[j].IsPointer {
				text += "*"
			}
			text += f.Returns[j].Type
			if j < len(f.Returns)-1 {
				text += ", "
			}
		}
		if len(f.Returns) > 1 {
			text += ") "
		}
		text += "{\n"

		for i := range f.Lines {
			text += "    " + f.Lines[i] + "\n"
		}

		text += "}\n\n"
	}

	// create or truncate the file
	file, err := os.Create(filepath.Join(folder, source.Name+".go"))
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(text)
	if err != nil {
		return err
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

func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

//======================================================================================
//     DTO Generation
//======================================================================================

func removeExistingFiles(folder string) error {
	err := filepath.Walk(folder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && filepath.Ext(path) == ".go" {
			fmt.Printf("Removing %s\n", path)
			if err := os.Remove(path); err != nil {
				fmt.Printf("Error removing %s\n", path)
				return fmt.Errorf("error removing %s: %s", path, err)
			}
		}
		return nil
	})

	return err
}

func generateConnector(connInfo *ConnectionInfo, folder string, packageName string) error {
	source := GoSourceFile{
		Name:    "db_connector",
		Package: packageName,
		Imports: []string{"context", "github.com/jackc/pgx/v5", "fmt"},
		Structs: make([]GoStruct, 0),
		Funcs:   make([]GoFuncs, 0),
	}

	// Func for connection
	connString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		connInfo.Host, connInfo.Port, connInfo.Username, connInfo.Password, connInfo.Database)

	connectFunc := GoFuncs{
		Name:    "Connect",
		Args:    make([]GoFuncArg, 0),
		Returns: make([]GoFuncReturn, 0),
		Lines:   make([]string, 0),
	}
	connectFunc.addArg(GoFuncArg{
		Name:      "connectionUrl",
		Type:      "string",
		IsPointer: true,
	})
	connectFunc.addReturn(GoFuncReturn{
		Type:      "pgx.Conn",
		IsPointer: true,
	})
	connectFunc.addReturn(GoFuncReturn{
		Type:      "error",
		IsPointer: false,
	})

	connectFunc.addLine("if connectionUrl == nil {")
	connectFunc.addLine("    defaultUrl := \"" + connString + "\"")
	connectFunc.addLine("    connectionUrl = &defaultUrl")
	connectFunc.addLine("}")
	connectFunc.addLine("conn, err := pgx.Connect(context.Background(), *connectionUrl)")
	connectFunc.addLine("if err != nil {")
	connectFunc.addLine("    return nil, fmt.Errorf(\"error connecting to postgres: %w\", err)")
	connectFunc.addLine("}")
	connectFunc.addLine("return conn, nil")
	source.addFunc(connectFunc)

	// Func for disconnection
	disconnectFunc := GoFuncs{
		Name:    "Disconnect",
		Args:    make([]GoFuncArg, 0),
		Returns: make([]GoFuncReturn, 0),
		Lines:   make([]string, 0),
	}
	disconnectFunc.addArg(GoFuncArg{
		Name:      "connection",
		Type:      "pgx.Conn",
		IsPointer: true,
	})
	disconnectFunc.addLine("connection.Close(context.Background())")
	source.addFunc(disconnectFunc)

	err := writeSource(folder, source)
	if err != nil {
		return err
	}

	return nil
}

func generateDTO(folder string, packageName string, table Table) error {
	fmt.Printf("Generating DTO for %s.%s\n", table.Schema, table.Name)

	source := GoSourceFile{
		Name:    table.Name,
		Package: packageName,
		Imports: make([]string, 0),
		Structs: make([]GoStruct, 0),
		Funcs:   make([]GoFuncs, 0),
	}

	entity := GoStruct{
		Name:   ToPascalCase(table.Name),
		Fields: make([]GoStructField, 0),
	}
	for i := range table.Columns {
		datatype := table.Columns[i].Datatype
		gotype, exists := PostgreSQLTypes[datatype]
		if !exists {
			gotype = datatype
		}
		if strings.HasPrefix(gotype, "time.") && !containsString(source.Imports, "time") {
			source.addImport("time")
		}

		entity.addField(GoStructField{
			Name:      ToPascalCase(table.Columns[i].Name),
			Type:      gotype,
			IsPointer: table.Columns[i].Nullable,
		})
	}
	source.addStruct(entity)

	err := writeSource(folder, source)
	if err != nil {
		return err
	}

	return nil
}

func writeGolang(connInfo *ConnectionInfo, folder string, metadata *Metadata) error {
	fmt.Println("Writing metadata to ", folder)

	// remove existing .go files in the target directory
	err := removeExistingFiles(folder)
	if err != nil {
		return err
	}

	parts := strings.Split(folder, "/")
	packageName := parts[len(parts)-1]

	// generate connector source file
	err = generateConnector(connInfo, folder, packageName)
	if err != nil {
		return err
	}

	// generate source files for each table
	for i := range metadata.Tables {
		err = generateDTO(folder, packageName, metadata.Tables[i])
		if err != nil {
			return err
		}
	}

	return nil
}
