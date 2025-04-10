package main

import (
    "fmt"
    "os"
    "os/exec"
    "path/filepath"
    "strings"
)

// ======================================================================================
//     Metaprogramming
// ======================================================================================

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
    Name       string
    Type       string
    IsPointer  bool
    Annotation *GoStructFieldAnnotation
}

type GoStructFieldAnnotation struct {
    Name  string
    Value string
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

func writeGoSource(folder string, source GoSourceFile) error {
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
        currStruct := source.Structs[i]
        text += "type " + currStruct.Name + " struct {\n"
        for j := range currStruct.Fields {
            currField := currStruct.Fields[j]
            text += "    " + currField.Name + " "
            if currField.IsPointer {
                text += "*"
            }
            text += currField.Type
            if currField.Annotation != nil {
                text += " `" + currField.Annotation.Name + ":\"" + currField.Annotation.Value + "\"`"
            }
            text += "\n"
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
    filePath := filepath.Join(folder, source.Name+".go")
    file, err := os.Create(filePath)
    if err != nil {
        return err
    }

    _, err = file.WriteString(text)
    if err != nil {
        return err
    }

    file.Close()
    // formatSource(filePath)

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

func containsString(slice []string, s string) bool {
    for _, item := range slice {
        if item == s {
            return true
        }
    }
    return false
}

func formatSource(filepath string) {
    cmd := exec.Command("gofmt", "-w", filepath)
    _, err := cmd.Output()
    if err != nil {
        fmt.Println("Error formatting ", err)
    }
}

// ======================================================================================
//     DTO Generation
// ======================================================================================

func addIfErr(f *GoFuncs, msg string, identLevel int) {
    prefix := ""
    if identLevel == 1 {
        prefix = "    "
    } else if identLevel == 2 {
        prefix = "        "
    } else if identLevel == 3 {
        prefix = "            "
    }

    f.addLine(prefix + "if err != nil {")
    f.addLine(prefix + "    return nil, fmt.Errorf(\"" + msg + "\", err)")
    f.addLine(prefix + "}")
}

func removeExistingGoFiles(folder string) error {
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

func generateGoDbConnector(connInfo *ConnectionInfo, folder string, packageName string) error {
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
    connectFunc.addArg(GoFuncArg{Name: "connectionUrl", Type: "string", IsPointer: true})
    connectFunc.addReturn(GoFuncReturn{Type: "pgx.Conn", IsPointer: true})
    connectFunc.addReturn(GoFuncReturn{Type: "error", IsPointer: false})

    connectFunc.addLine("if connectionUrl == nil {")
    connectFunc.addLine("    defaultUrl := \"" + connString + "\"")
    connectFunc.addLine("    connectionUrl = &defaultUrl")
    connectFunc.addLine("}")
    connectFunc.addLine("conn, err := pgx.Connect(context.Background(), *connectionUrl)")
    addIfErr(&connectFunc, "error connecting to postgres: %w", 0)
    connectFunc.addLine("return conn, nil")
    source.addFunc(connectFunc)

    // Func for disconnection
    disconnectFunc := GoFuncs{
        Name:    "Disconnect",
        Args:    make([]GoFuncArg, 0),
        Returns: make([]GoFuncReturn, 0),
        Lines:   make([]string, 0),
    }
    disconnectFunc.addArg(GoFuncArg{Name: "connection", Type: "pgx.Conn", IsPointer: true})
    disconnectFunc.addLine("connection.Close(context.Background())")
    source.addFunc(disconnectFunc)

    err := writeGoSource(folder, source)
    if err != nil {
        return err
    }

    return nil
}

func generateTableStruct(table *Table, source *GoSourceFile) error {
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
            Annotation: &GoStructFieldAnnotation{
                Name:  "json",
                Value: table.Columns[i].Name,
            },
        })
    }
    source.addStruct(entity)

    return nil
}

func generateScanRow(table *Table, source *GoSourceFile) error {
    tableNamePascalCase := ToPascalCase(table.Name)
    tableNameCamelCase := ToCamelCase(table.Name)

    // function that receives *pgx.Rows and scan one row
    scanRowFunc := GoFuncs{
        Name:    "Scan" + tableNamePascalCase + "Row",
        Args:    make([]GoFuncArg, 0),
        Returns: make([]GoFuncReturn, 0),
        Lines:   make([]string, 0),
    }
    scanRowFunc.addArg(GoFuncArg{Name: "rows", Type: "pgx.Rows", IsPointer: true})
    scanRowFunc.addReturn(GoFuncReturn{Type: tableNamePascalCase, IsPointer: true})
    scanRowFunc.addReturn(GoFuncReturn{Type: "error", IsPointer: false})

    scanRowFunc.addLine("var " + tableNameCamelCase + " " + tableNamePascalCase)
    scanRowFunc.addLine("err := (*rows).Scan(")

    for i := range source.Structs[0].Fields {
        sf := source.Structs[0].Fields[i]
        if i == len(source.Structs[0].Fields)-1 {
            scanRowFunc.addLine("    &" + tableNameCamelCase + "." + sf.Name + ")")
        } else {
            scanRowFunc.addLine("    &" + tableNameCamelCase + "." + sf.Name + ",")
        }
    }

    addIfErr(&scanRowFunc, "error scanning row: %w", 0)
    scanRowFunc.addLine("return &" + tableNameCamelCase + ", nil")

    source.addFunc(scanRowFunc)

    // function that receives *pgx.Row and scan one row
    scanRowFunc = GoFuncs{
        Name:    "ScanSingle" + tableNamePascalCase + "Row",
        Args:    make([]GoFuncArg, 0),
        Returns: make([]GoFuncReturn, 0),
        Lines:   make([]string, 0),
    }
    scanRowFunc.addArg(GoFuncArg{Name: "row", Type: "pgx.Row", IsPointer: true})
    scanRowFunc.addReturn(GoFuncReturn{Type: tableNamePascalCase, IsPointer: true})
    scanRowFunc.addReturn(GoFuncReturn{Type: "error", IsPointer: false})

    scanRowFunc.addLine("var " + tableNameCamelCase + " " + tableNamePascalCase)
    scanRowFunc.addLine("err := (*row).Scan(")

    for i := range source.Structs[0].Fields {
        sf := source.Structs[0].Fields[i]
        if i == len(source.Structs[0].Fields)-1 {
            scanRowFunc.addLine("    &" + tableNameCamelCase + "." + sf.Name + ")")
        } else {
            scanRowFunc.addLine("    &" + tableNameCamelCase + "." + sf.Name + ",")
        }
    }

    addIfErr(&scanRowFunc, "error scanning row: %w", 0)
    scanRowFunc.addLine("return &" + tableNameCamelCase + ", nil")

    source.addFunc(scanRowFunc)

    return nil
}

func generateScanMultipleRows(table *Table, source *GoSourceFile) error {
    tableNamePascalCase := ToPascalCase(table.Name)
    tableNameCamelCase := ToCamelCase(table.Name)
    scanMultiRowsFunc := GoFuncs{
        Name:    "ScanAll" + tableNamePascalCase + "Rows",
        Args:    make([]GoFuncArg, 0),
        Returns: make([]GoFuncReturn, 0),
        Lines:   make([]string, 0),
    }
    scanMultiRowsFunc.addArg(GoFuncArg{Name: "rows", Type: "pgx.Rows", IsPointer: true})
    scanMultiRowsFunc.addReturn(GoFuncReturn{Type: "[]" + tableNamePascalCase, IsPointer: false})
    scanMultiRowsFunc.addReturn(GoFuncReturn{Type: "error", IsPointer: false})

    scanMultiRowsFunc.addLine("var results []" + tableNamePascalCase)
    scanMultiRowsFunc.addLine("for (*rows).Next() {")
    scanMultiRowsFunc.addLine("    " + tableNameCamelCase + ", err := Scan" + tableNamePascalCase + "Row(rows)")
    scanMultiRowsFunc.addLine("    if err != nil {")
    scanMultiRowsFunc.addLine("        return nil, fmt.Errorf(\"error scanning row\")")
    scanMultiRowsFunc.addLine("    }")
    scanMultiRowsFunc.addLine("    results = append(results, *" + tableNameCamelCase + ")")
    scanMultiRowsFunc.addLine("}\n")
    scanMultiRowsFunc.addLine("err := (*rows).Err()")
    addIfErr(&scanMultiRowsFunc, "error scanning row: %w", 0)
    scanMultiRowsFunc.addLine("return results, nil")

    source.addFunc(scanMultiRowsFunc)
    return nil
}

func generateSelectAll(table *Table, source *GoSourceFile) error {
    tableNamePascalCase := ToPascalCase(table.Name)
    selectAllFunc := GoFuncs{
        Name:    "SelectAll" + tableNamePascalCase,
        Args:    make([]GoFuncArg, 0),
        Returns: make([]GoFuncReturn, 0),
        Lines:   make([]string, 0),
    }
    selectAllFunc.addArg(GoFuncArg{Name: "conn", Type: "pgx.Conn", IsPointer: true})
    selectAllFunc.addArg(GoFuncArg{Name: "limit", Type: "uint", IsPointer: false})
    selectAllFunc.addArg(GoFuncArg{Name: "offset", Type: "uint", IsPointer: false})
    selectAllFunc.addReturn(GoFuncReturn{Type: "[]" + tableNamePascalCase, IsPointer: false})
    selectAllFunc.addReturn(GoFuncReturn{Type: "error", IsPointer: false})

    sql := "SELECT * FROM " + table.Name + " LIMIT $1 OFFSET $2"
    selectAllFunc.addLine("rows, err := conn.Query(context.Background(), \"" + sql + "\", limit, offset)")
    addIfErr(&selectAllFunc, "error scanning row: %w", 0)
    selectAllFunc.addLine("defer rows.Close()")
    selectAllFunc.addLine("")
    selectAllFunc.addLine("return ScanAll" + tableNamePascalCase + "Rows(&rows)")

    source.addFunc(selectAllFunc)
    return nil
}

func generateSelectByPK(table *Table, source *GoSourceFile) error {
    tableNamePascalCase := ToPascalCase(table.Name)
    selectByPKFunc := GoFuncs{
        Name:    "Select" + tableNamePascalCase + "ByPK",
        Args:    make([]GoFuncArg, 0),
        Returns: make([]GoFuncReturn, 0),
        Lines:   make([]string, 0),
    }
    selectByPKFunc.addArg(GoFuncArg{Name: "conn", Type: "pgx.Conn", IsPointer: true})

    var pks []*Column
    for i := range table.Columns {
        var col = table.Columns[i]
        if col.IsPrimaryKey {
            pks = append(pks, &col)
        }
    }

    for i := range pks {
        selectByPKFunc.addArg(GoFuncArg{
            Name:      ToCamelCase(pks[i].Name),
            Type:      PostgreSQLTypes[pks[i].Datatype],
            IsPointer: false,
        })
    }

    selectByPKFunc.addReturn(GoFuncReturn{Type: tableNamePascalCase, IsPointer: true})
    selectByPKFunc.addReturn(GoFuncReturn{Type: "error", IsPointer: false})

    sql := "SELECT * FROM " + table.Name + " WHERE true "
    count := 0
    for i := range table.Columns {
        var col = table.Columns[i]
        if col.IsPrimaryKey {
            count += 1
            sql += fmt.Sprintf(" AND %s = $%d", col.Name, count)
        }
    }
    selectByPKFunc.addLine("row := conn.QueryRow(")
    selectByPKFunc.addLine("    context.Background(),")
    selectByPKFunc.addLine("    \"" + sql + "\",")

    for i := range pks {
        if i < len(pks)-1 {
            selectByPKFunc.addLine("    " + ToCamelCase(pks[i].Name) + ",")
        } else {
            selectByPKFunc.addLine("    " + ToCamelCase(pks[i].Name) + ")")
        }
    }

    selectByPKFunc.addLine("return ScanSingle" + tableNamePascalCase + "Row(&row)")

    source.addFunc(selectByPKFunc)
    return nil
}

func generateSelectByCol(col *Column, table *Table, source *GoSourceFile) error {
    tableNamePascalCase := ToPascalCase(table.Name)
    selectByColFunc := GoFuncs{
        Name:    "SelectAll" + tableNamePascalCase + "By" + ToPascalCase(col.Name),
        Args:    make([]GoFuncArg, 0),
        Returns: make([]GoFuncReturn, 0),
        Lines:   make([]string, 0),
    }
    selectByColFunc.addArg(GoFuncArg{Name: "conn", Type: "pgx.Conn", IsPointer: true})

    argName := ToCamelCase(col.Name)
    if col.Name == "type" {
        argName += "1"
    }
    selectByColFunc.addArg(GoFuncArg{Name: argName, Type: PostgreSQLTypes[col.Datatype], IsPointer: false})
    selectByColFunc.addReturn(GoFuncReturn{Type: "[]" + tableNamePascalCase, IsPointer: false})
    selectByColFunc.addReturn(GoFuncReturn{Type: "error", IsPointer: false})

    sql := "SELECT * FROM " + table.Name + " WHERE " + col.Name + " = $1"
    selectByColFunc.addLine("rows, err := conn.Query(context.Background(), \"" + sql + "\", " + argName + ")")
    addIfErr(&selectByColFunc, "error scanning row: %w", 0)
    selectByColFunc.addLine("defer rows.Close()")
    selectByColFunc.addLine("")
    selectByColFunc.addLine("return ScanAll" + tableNamePascalCase + "Rows(&rows)")

    source.addFunc(selectByColFunc)
    return nil
}

func generateInsert(table *Table, source *GoSourceFile) error {
    tableNamePascalCase := ToPascalCase(table.Name)
    tableNameCamelCase := ToCamelCase(table.Name)

    var autoIncrementCol *Column
    for i := range table.Columns {
        if table.Columns[i].IsAutoIncrement {
            autoIncrementCol = &table.Columns[i]
            break
        }
    }

    insertFunc := GoFuncs{
        Name:    "Insert" + tableNamePascalCase,
        Args:    make([]GoFuncArg, 0),
        Returns: make([]GoFuncReturn, 0),
        Lines:   make([]string, 0),
    }
    insertFunc.addArg(GoFuncArg{Name: "conn", Type: "pgx.Conn", IsPointer: true})
    insertFunc.addArg(GoFuncArg{Name: tableNameCamelCase, Type: tableNamePascalCase, IsPointer: true})
    insertFunc.addReturn(GoFuncReturn{Type: "error", IsPointer: false})

    insertFunc.addLine("query := `")
    insertFunc.addLine("    INSERT INTO " + table.Name + " (")
    for i := range table.Columns {
        if table.Columns[i].IsAutoIncrement {
            continue
        }
        if i < len(table.Columns)-1 {
            insertFunc.addLine("        " + table.Columns[i].Name + ",")
        } else {
            insertFunc.addLine("        " + table.Columns[i].Name + ")")
        }
    }
    insertFunc.addLine("    VALUES")
    term := ""
    count := 1
    for i := range table.Columns {
        if table.Columns[i].IsAutoIncrement {
            continue
        }
        if i < len(table.Columns)-1 {
            term += fmt.Sprintf("$%d,", count)
        } else {
            term += fmt.Sprintf("$%d)", count)
        }
        count += 1
    }
    insertFunc.addLine("        (" + term)
    for i := range table.Columns {
        if table.Columns[i].IsAutoIncrement {
            insertFunc.addLine("    RETURNING " + table.Columns[i].Name)
            break
        }
    }
    insertFunc.addLine("`\n")

    if autoIncrementCol != nil {
        insertFunc.addLine("row := conn.QueryRow(context.Background(), query,")
    } else {
        insertFunc.addLine("_ = conn.QueryRow(context.Background(), query,")
    }
    for i := range table.Columns {
        if table.Columns[i].IsAutoIncrement {
            continue
        }
        if i < len(table.Columns)-1 {
            insertFunc.addLine("    " + tableNameCamelCase + "." + ToPascalCase(table.Columns[i].Name) + ",")
        } else {
            insertFunc.addLine("    " + tableNameCamelCase + "." + ToPascalCase(table.Columns[i].Name) + ")")
        }
    }
    if autoIncrementCol != nil {
        insertFunc.addLine("")
        insertFunc.addLine("var " + ToCamelCase(autoIncrementCol.Name) + " " + PostgreSQLTypes[autoIncrementCol.Datatype])
        insertFunc.addLine("err := row.Scan(&" + ToCamelCase(autoIncrementCol.Name) + ")")
        insertFunc.addLine("if err != nil {")
        insertFunc.addLine("    return nil")
        insertFunc.addLine("}")
        insertFunc.addLine(tableNameCamelCase + "." + ToPascalCase(autoIncrementCol.Name) + " = " + ToCamelCase(autoIncrementCol.Name))
    }

    insertFunc.addLine("return nil")

    source.addFunc(insertFunc)
    return nil
}

func generateUpdate(table *Table, source *GoSourceFile) error {
    tableNamePascalCase := ToPascalCase(table.Name)
    tableNameCamelCase := ToCamelCase(table.Name)

    var primaryKeys []*Column
    for i := range table.Columns {
        if table.Columns[i].IsPrimaryKey {
            primaryKeys = append(primaryKeys, &table.Columns[i])
        }
    }

    updateFunc := GoFuncs{
        Name:    "Update" + tableNamePascalCase,
        Args:    make([]GoFuncArg, 0),
        Returns: make([]GoFuncReturn, 0),
        Lines:   make([]string, 0),
    }
    updateFunc.addArg(GoFuncArg{Name: "conn", Type: "pgx.Conn", IsPointer: true})
    updateFunc.addArg(GoFuncArg{Name: tableNameCamelCase, Type: tableNamePascalCase, IsPointer: true})
    updateFunc.addReturn(GoFuncReturn{Type: "error", IsPointer: false})

    updateFunc.addLine("query := `")
    updateFunc.addLine("    UPDATE " + table.Name)
    updateFunc.addLine("    SET")
    count := 1
    for i := range table.Columns {
        if table.Columns[i].IsPrimaryKey {
            continue
        }
        if i < len(table.Columns)-1 {
            updateFunc.addLine("        " + table.Columns[i].Name + fmt.Sprintf(" = $%d,", count))
        } else {
            updateFunc.addLine("        " + table.Columns[i].Name + fmt.Sprintf(" = $%d", count))
        }
        count += 1
    }

    term := "    WHERE true"
    for i := range primaryKeys {
        term += " AND " + primaryKeys[i].Name + fmt.Sprintf(" = $%d", count)
    }
    updateFunc.addLine(term)
    updateFunc.addLine("`\n")

    updateFunc.addLine("_, err := conn.Exec(context.Background(), query,")
    for i := range table.Columns {
        if table.Columns[i].IsPrimaryKey {
            continue
        }
        updateFunc.addLine("    " + tableNameCamelCase + "." + ToPascalCase(table.Columns[i].Name) + ",")
    }
    for i := range primaryKeys {
        if i < len(primaryKeys)-1 {
            updateFunc.addLine("    " + tableNameCamelCase + "." + ToPascalCase(primaryKeys[i].Name) + ",")
        } else {
            updateFunc.addLine("    " + tableNameCamelCase + "." + ToPascalCase(primaryKeys[i].Name) + ")")
        }
    }

    updateFunc.addLine("if err != nil {")
    updateFunc.addLine("    return fmt.Errorf(\"failed to perform update: %w\", err)")
    updateFunc.addLine("}")
    updateFunc.addLine("return nil")

    source.addFunc(updateFunc)
    return nil
}

func generateDelete(table *Table, source *GoSourceFile) error {
    tableNamePascalCase := ToPascalCase(table.Name)
    tableNameCamelCase := ToCamelCase(table.Name)

    var primaryKeys []*Column
    for i := range table.Columns {
        if table.Columns[i].IsPrimaryKey {
            primaryKeys = append(primaryKeys, &table.Columns[i])
        }
    }

    updateFunc := GoFuncs{
        Name:    "Delete" + tableNamePascalCase,
        Args:    make([]GoFuncArg, 0),
        Returns: make([]GoFuncReturn, 0),
        Lines:   make([]string, 0),
    }
    updateFunc.addArg(GoFuncArg{Name: "conn", Type: "pgx.Conn", IsPointer: true})
    updateFunc.addArg(GoFuncArg{Name: tableNameCamelCase, Type: tableNamePascalCase, IsPointer: true})
    updateFunc.addReturn(GoFuncReturn{Type: "error", IsPointer: false})

    updateFunc.addLine("query := `")
    updateFunc.addLine("    DELETE FROM " + table.Name)
    term := "    WHERE "
    count := 1
    for i := range primaryKeys {
        if i > 0 {
            term += " AND "
        }
        term += primaryKeys[i].Name + fmt.Sprintf(" = $%d", count)
        count += 1
    }
    updateFunc.addLine(term)
    updateFunc.addLine("`\n")

    updateFunc.addLine("_, err := conn.Exec(context.Background(), query,")
    for i := range primaryKeys {
        if i < len(primaryKeys)-1 {
            updateFunc.addLine("    " + tableNameCamelCase + "." + ToPascalCase(primaryKeys[i].Name) + ",")
        } else {
            updateFunc.addLine("    " + tableNameCamelCase + "." + ToPascalCase(primaryKeys[i].Name) + ")")
        }
    }

    updateFunc.addLine("if err != nil {")
    updateFunc.addLine("    return fmt.Errorf(\"failed to perform update: %w\", err)")
    updateFunc.addLine("}")
    updateFunc.addLine("return nil")

    source.addFunc(updateFunc)
    return nil
}

func generateExists(table *Table, source *GoSourceFile) error {
    tableNamePascalCase := ToPascalCase(table.Name)

    var primaryKeys []*Column
    for i := range table.Columns {
        if table.Columns[i].IsPrimaryKey {
            primaryKeys = append(primaryKeys, &table.Columns[i])
        }
    }

    existsFunc := GoFuncs{
        Name:    "Exists" + tableNamePascalCase,
        Args:    make([]GoFuncArg, 0),
        Returns: make([]GoFuncReturn, 0),
        Lines:   make([]string, 0),
    }
    existsFunc.addArg(GoFuncArg{Name: "conn", Type: "pgx.Conn", IsPointer: true})

    for i := range primaryKeys {
        existsFunc.addArg(GoFuncArg{
            Name:      ToCamelCase(primaryKeys[i].Name),
            Type:      PostgreSQLTypes[primaryKeys[i].Datatype],
            IsPointer: false,
        })
    }

    existsFunc.addReturn(GoFuncReturn{Type: "bool", IsPointer: false})
    existsFunc.addReturn(GoFuncReturn{Type: "error", IsPointer: false})

    existsFunc.addLine("query := `")
    existsFunc.addLine("    SELECT count(*)")
    existsFunc.addLine("    FROM " + table.Name)
    term := "    WHERE "
    count := 1
    for i := range primaryKeys {
        if i > 0 {
            term += " AND "
        }
        term += primaryKeys[i].Name + fmt.Sprintf(" = $%d", count)
        count += 1
    }
    existsFunc.addLine(term)
    existsFunc.addLine("`\n")

    term = "row := conn.QueryRow(context.Background(), query"
    for i := range primaryKeys {
        term += ", " + ToCamelCase(primaryKeys[i].Name)
    }
    existsFunc.addLine(term + ")")

    existsFunc.addLine("var exists int64 = 0")
    existsFunc.addLine("err := row.Scan(&exists)")
    existsFunc.addLine("if err != nil {")
    existsFunc.addLine("    return false, fmt.Errorf(\"failed to perform exists: %w\", err)")
    existsFunc.addLine("}")
    existsFunc.addLine("return exists > 0, nil")

    source.addFunc(existsFunc)
    return nil
}

func generateUpsert(table *Table, source *GoSourceFile) error {
    tableNamePascalCase := ToPascalCase(table.Name)
    tableNameCamelCase := ToCamelCase(table.Name)

    var primaryKeys []*Column
    for i := range table.Columns {
        if table.Columns[i].IsPrimaryKey {
            primaryKeys = append(primaryKeys, &table.Columns[i])
        }
    }

    upsertFunc := GoFuncs{
        Name:    "Upsert" + tableNamePascalCase,
        Args:    make([]GoFuncArg, 0),
        Returns: make([]GoFuncReturn, 0),
        Lines:   make([]string, 0),
    }
    upsertFunc.addArg(GoFuncArg{Name: "conn", Type: "pgx.Conn", IsPointer: true})
    upsertFunc.addArg(GoFuncArg{Name: tableNameCamelCase, Type: tableNamePascalCase, IsPointer: true})
    upsertFunc.addReturn(GoFuncReturn{Type: "error", IsPointer: false})

    term := "exists, err := Exists" + tableNamePascalCase + "(conn"
    for i := range primaryKeys {
        term += ", " + tableNameCamelCase + "." + ToPascalCase(primaryKeys[i].Name)
    }
    upsertFunc.addLine(term + ")")

    upsertFunc.addLine("if err != nil {")
    upsertFunc.addLine("    return err")
    upsertFunc.addLine("}")

    upsertFunc.addLine("if exists {")
    upsertFunc.addLine("    err = Update" + tableNamePascalCase + "(conn, " + tableNameCamelCase + ")")
    upsertFunc.addLine("    if err != nil {")
    upsertFunc.addLine("        return err")
    upsertFunc.addLine("    }")
    upsertFunc.addLine("} else {")
    upsertFunc.addLine("    err = Insert" + tableNamePascalCase + "(conn, " + tableNameCamelCase + ")")
    upsertFunc.addLine("    if err != nil {")
    upsertFunc.addLine("        return err")
    upsertFunc.addLine("    }")
    upsertFunc.addLine("}")
    upsertFunc.addLine("return nil")

    source.addFunc(upsertFunc)
    return nil
}

func generateDTO(folder string, packageName string, table Table) error {
    fmt.Printf("Generating DTO for %s.%s\n", table.Schema, table.Name)

    // init go source struct
    source := GoSourceFile{
        Name:    table.Name,
        Package: packageName,
        Imports: []string{"context", "fmt", "github.com/jackc/pgx/v5"},
        Structs: make([]GoStruct, 0),
        Funcs:   make([]GoFuncs, 0),
    }

    // generate table struct used throughout the dto
    err := generateTableStruct(&table, &source)
    if err != nil {
        return err
    }

    // generate scan row func
    err = generateScanRow(&table, &source)
    if err != nil {
        return err
    }

    // generate scan multiple rows func
    err = generateScanMultipleRows(&table, &source)
    if err != nil {
        return err
    }

    // generate select all func
    err = generateSelectAll(&table, &source)
    if err != nil {
        return err
    }

    // generate select by pk
    err = generateSelectByPK(&table, &source)
    if err != nil {
        return err
    }

    // generate select by columns that are not pk
    for i := range table.Columns {
        if table.Columns[i].IsPrimaryKey {
            continue
        }
        err = generateSelectByCol(&table.Columns[i], &table, &source)
        if err != nil {
            return err
        }
    }

    // generate insert
    err = generateInsert(&table, &source)
    if err != nil {
        return err
    }

    // generate update
    err = generateUpdate(&table, &source)
    if err != nil {
        return err
    }

    // generate delete
    err = generateDelete(&table, &source)
    if err != nil {
        return err
    }

    // if table has no autoinc col, we can generate exists query and upsert
    hasAutoinc := false
    for i := range table.Columns {
        if table.Columns[i].IsAutoIncrement {
            hasAutoinc = true
            break
        }
    }
    if !hasAutoinc {
        // generate exists query
        err = generateExists(&table, &source)
        if err != nil {
            return err
        }

        // generate upsert query
        err = generateUpsert(&table, &source)
        if err != nil {
            return err
        }
    }

    // write final text file
    err = writeGoSource(folder, source)
    if err != nil {
        return err
    }

    return nil
}

func generateCustomQueries(folder string, packageName string, metadata *Metadata, customQueries []CustomQuery) error {
    fmt.Printf("Generating custom queries file")

    // init go source struct
    source := GoSourceFile{
        Name:    "custom_queries",
        Package: packageName,
        Imports: []string{"context", "github.com/jackc/pgx/v5"},
        Structs: make([]GoStruct, 0),
        Funcs:   make([]GoFuncs, 0),
    }

    for i := range customQueries {
        var cq = customQueries[i]

        resS := GoStruct{
            Name:   ToPascalCase(cq.Name) + "Result",
            Fields: make([]GoStructField, 0),
        }

        // fill struct fields
        if len(cq.ProjectionColumns) > 1 {
            for j := range cq.ProjectionColumns {
                col := cq.ProjectionColumns[j]
                // get table referenced by column
                var table *Table
                for k := range metadata.Tables {
                    if col.Table == metadata.Tables[k].Name {
                        table = &metadata.Tables[k]
                    }
                }
                // iterate over cols of table, adding to struct if necessary
                for k := range table.Columns {
                    if col.Column == "*" || col.Column == table.Columns[k].Name {
                        prefix := ""
                        if col.Nullable {
                            prefix = "*"
                        }
                        resS.addField(GoStructField{
                            Name: ToPascalCase(col.Table) + ToPascalCase(table.Columns[k].Name),
                            Type: prefix + PostgreSQLTypes[table.Columns[k].Datatype],
                            Annotation: &GoStructFieldAnnotation{
                                Name:  "json",
                                Value: col.Table + "_" + col.Column,
                            },
                        })
                    }
                }
            }
        }

        qf := GoFuncs{
            Name:    cq.Name,
            Args:    make([]GoFuncArg, 0),
            Returns: make([]GoFuncReturn, 0),
            Lines:   make([]string, 0),
        }

        // add func args
        qf.addArg(GoFuncArg{Name: "conn", Type: "pgx.Conn", IsPointer: true})
        for j := range cq.Parameters {
            var p = cq.Parameters[j]
            qf.addArg(GoFuncArg{Name: p.ParamName, Type: p.GoType, IsPointer: false})
        }

        // add func returns
        projectionType := ""
        projIsPrimitiveType := true
        if len(cq.ProjectionColumns) == 0 {
        } else if len(cq.ProjectionColumns) == 1 {
            col := cq.ProjectionColumns[0]
            if col.Table == "" {
                projectionType = PostgreSQLTypes[col.SQLType]
            } else if col.Table != "" && col.Column != "*" {
                tableRef := metadata.searchTableByName(col.Table)
                columnRef := tableRef.searchColumnByName(col.Column)
                projectionType = PostgreSQLTypes[columnRef.Datatype]
                projIsPrimitiveType = true
            } else {
                projectionType = ToPascalCase(col.Table)
                projIsPrimitiveType = false
            }
        } else {
            projectionType = resS.Name
            projIsPrimitiveType = false
        }
        if cq.Cardinality == "1" {
            qf.addReturn(GoFuncReturn{Type: projectionType, IsPointer: !projIsPrimitiveType})
        } else if cq.Cardinality == "N" {
            qf.addReturn(GoFuncReturn{Type: "[]" + projectionType, IsPointer: false})
        }
        qf.addReturn(GoFuncReturn{Type: "error", IsPointer: false})

        // add sql text
        qf.addLine("query := `")
        for j := range cq.SQLText {
            qf.addLine(cq.SQLText[j])
        }
        qf.addLine("`")

        // perform query
        params := ""
        for i := range cq.Parameters {
            params += ", " + cq.Parameters[i].ParamName
        }
        if cq.Cardinality == "0" {
            qf.addLine("_, err := conn.Exec(context.Background(), query" + params + ")")
            qf.addLine("if err != nil {")
            qf.addLine("    return err")
            qf.addLine("}")
        } else if cq.Cardinality == "1" {
            qf.addLine("row := conn.QueryRow(context.Background(), query" + params + ")")
        } else if cq.Cardinality == "N" {
            qf.addLine("rows, err := conn.Query(context.Background(), query" + params + ")")
            qf.addLine("if err != nil {")
            qf.addLine("    return nil, err")
            qf.addLine("}")
        }

        // scan and return result
        if cq.Cardinality == "0" {
            qf.addLine("return nil")
        } else if cq.Cardinality == "1" {
            if !projIsPrimitiveType && !strings.HasSuffix(projectionType, "Result") {
                qf.addLine("return ScanSingle" + projectionType + "Row(&row)")
            } else {
                qf.addLine("var result " + projectionType)

                if projIsPrimitiveType {
                    qf.addLine("err := row.Scan(&result)")
                    qf.addLine("if err != nil {")
                    qf.addLine("    return result, err")
                    qf.addLine("}")
                    qf.addLine("return result, nil")
                } else {
                    qf.addLine("err := row.Scan(")
                    for i := range resS.Fields {
                        if i < len(resS.Fields)-1 {
                            qf.addLine("    &result." + resS.Fields[i].Name + ",")
                        } else {
                            qf.addLine("    &result." + resS.Fields[i].Name + ")")
                        }
                    }
                    qf.addLine("if err != nil {")
                    qf.addLine("    return nil, err")
                    qf.addLine("}")
                    qf.addLine("return &result, nil")
                }
            }
        } else if cq.Cardinality == "N" {
            if !projIsPrimitiveType && !strings.HasSuffix(projectionType, "Result") {
                qf.addLine("return ScanAll" + projectionType + "Rows(&rows)")
            } else {
                qf.addLine("var results []" + projectionType)
                qf.addLine("for rows.Next() {")
                qf.addLine("    var res " + projectionType)
                qf.addLine("    err := rows.Scan(")

                if projIsPrimitiveType {
                    qf.addLine("        &res)")
                } else {
                    for i := range resS.Fields {
                        if i < len(resS.Fields)-1 {
                            qf.addLine("        &res." + resS.Fields[i].Name + ",")
                        } else {
                            qf.addLine("        &res." + resS.Fields[i].Name + ")")
                        }
                    }
                }

                qf.addLine("    if err != nil {")
                qf.addLine("        return nil, err")
                qf.addLine("    }")
                qf.addLine("    results = append(results, res)")
                qf.addLine("}")
                qf.addLine("return results, nil")
            }
        }

        // add parts to source file
        if len(resS.Fields) > 0 {
            source.addStruct(resS)
        }
        source.addFunc(qf)
    }

    // write final text file
    err := writeGoSource(folder, source)
    if err != nil {
        return err
    }

    return nil
}

func writeGolang(connInfo *ConnectionInfo, folder string, metadata *Metadata, customQueries []CustomQuery) error {
    fmt.Println("Generating DTO files on ", folder)

    // remove existing .go files in the target directory
    err := removeExistingGoFiles(folder)
    if err != nil {
        return err
    }

    parts := strings.Split(folder, "/")
    packageName := parts[len(parts)-1]

    // generate connector source file
    err = generateGoDbConnector(connInfo, folder, packageName)
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

    // generate custom queries file
    err = generateCustomQueries(folder, packageName, metadata, customQueries)
    if err != nil {
        return err
    }

    return nil
}
