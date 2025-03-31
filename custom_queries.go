package main

import (
    "bufio"
    "fmt"
    "os"
    "path/filepath"
    "strings"
)

type ProjectionColumn struct {
    Table    string
    Column   string
    SQLType  string
    Nullable bool
}

type QueryParameter struct {
    ParamName string
    GoType    string
}

type CustomQuery struct {
    Name              string
    Cardinality       string
    ProjectionColumns []ProjectionColumn
    Parameters        []QueryParameter
    SQLText           []string
}

func splitConfFile(confFile string) ([][]string, error) {
    // ------------------------------------------------
    //  READ CONF FILE INTO LINES
    // ------------------------------------------------

    file, err := os.Open(confFile)
    if err != nil {
        return nil, err
    }
    defer file.Close()

    scanner := bufio.NewScanner(file)

    var lines []string
    for scanner.Scan() {
        lines = append(lines, scanner.Text())
    }
    if err := scanner.Err(); err != nil {
        return nil, err
    }

    // ------------------------------------------------
    //  GROUP LINES INTO QUERIES
    // ------------------------------------------------

    queries := make([][]string, 0)
    queryLines := make([]string, 0)
    for i := range lines {
        if strings.Trim(lines[i], " \t\v\n") == "" {
            continue
        }
        if strings.HasPrefix(lines[i], "[query]") {
            if len(queryLines) > 0 {
                queries = append(queries, queryLines)
            }
            queryLines = make([]string, 0)
            queryLines = append(queryLines, lines[i])
        } else {
            queryLines = append(queryLines, lines[i])
        }
    }
    queries = append(queries, queryLines)

    return queries, nil
}

const (
    StateTop = iota
    StateProjection
    StateParameters
    StateSQL
)

func parseQuery(confPart []string) (*CustomQuery, error) {
    // fmt.Println("Parsing...")
    // for i := range confPart {
    //     fmt.Println(" >>    " + confPart[i])
    // }

    customQuery := CustomQuery{
        Name:              "",
        Cardinality:       "",
        ProjectionColumns: make([]ProjectionColumn, 0),
        Parameters:        make([]QueryParameter, 0),
        SQLText:           make([]string, 0),
    }

    parserState := StateTop
    for i := range confPart {
        switch parserState {
        case StateTop:
            trimmedLine := strings.Trim(confPart[i], " \t\n\v")
            parts := strings.Split(trimmedLine, "=")
            if strings.HasPrefix(trimmedLine, "[query]") {
                continue
            } else if strings.HasPrefix(trimmedLine, "name=") {
                customQuery.Name = parts[1]
            } else if strings.HasPrefix(trimmedLine, "cardinality=") {
                if parts[1] != "0" && parts[1] != "1" && parts[1] != "N" {
                    return nil, fmt.Errorf("invalid cardinality value: %s", parts[1])
                }
                customQuery.Cardinality = parts[1]
            } else if strings.HasPrefix(trimmedLine, "projection=") {
                parserState = StateProjection
            } else if strings.HasPrefix(trimmedLine, "parameters=") {
                parserState = StateParameters
            } else if strings.HasPrefix(trimmedLine, "sql=") {
                parserState = StateSQL
            } else {
                return nil, fmt.Errorf("unexpected line: %s", confPart[i])
            }
        case StateProjection:
            trimmedLine := strings.Trim(confPart[i], " \t\n\v")
            if trimmedLine == "END" {
                parserState = StateTop
                continue
            }

            pcol := ProjectionColumn{
                Table:    "",
                Column:   "",
                SQLType:  "",
                Nullable: false,
            }

            if strings.HasSuffix(trimmedLine, "NULL") {
                pcol.Nullable = true
                trimmedLine = strings.Replace(trimmedLine, "NULL", "", 1)
                trimmedLine = strings.TrimSpace(trimmedLine)
            }

            parts := strings.Split(trimmedLine, " ")
            if len(parts) == 1 {
                // refers to an existing table/column
                tc := strings.Split(parts[0], ".")
                if len(tc) != 2 {
                    return nil, fmt.Errorf("invalid projection column: %s", confPart[i])
                }
                pcol.Table = tc[0]
                pcol.Column = tc[1]
            } else if len(parts) == 2 {
                // refers to column created by the query
                pcol.Column = parts[0]
                pcol.SQLType = parts[1]
            }

            customQuery.ProjectionColumns = append(customQuery.ProjectionColumns, pcol)
        case StateParameters:
            trimmedLine := strings.Trim(confPart[i], " \t\n\v")
            if trimmedLine == "END" {
                parserState = StateTop
                continue
            }

            parts := strings.Split(trimmedLine, " ")
            if len(parts) != 2 {
                return nil, fmt.Errorf("invalid parameter value: %s", confPart[i])
            }

            customQuery.Parameters = append(customQuery.Parameters, QueryParameter{
                ParamName: parts[0],
                GoType:    parts[1],
            })
        case StateSQL:
            trimmedLine := strings.Trim(confPart[i], " \t\n\v")
            if trimmedLine == "END" {
                parserState = StateTop
                continue
            }

            customQuery.SQLText = append(customQuery.SQLText, confPart[i])
        default:
            return nil, fmt.Errorf("unknown parser state: %d", parserState)
        }
    }

    return &customQuery, nil
}

func readCustomQueries(folder string) ([]CustomQuery, error) {
    // Build path to config file
    confFile := filepath.Join(folder, "custom_queries.conf")
    _, err := os.Stat(confFile)
    if os.IsNotExist(err) {
        fmt.Println("custom queries file does not exist")
        return nil, nil
    }

    // split conf file into queries
    parts, err := splitConfFile(confFile)
    if err != nil {
        return nil, err
    }

    // parse each query
    var queries []CustomQuery
    for i := range parts {
        q, err := parseQuery(parts[i])
        if err != nil {
            return nil, err
        }
        _ = q
        queries = append(queries, *q)
    }
    fmt.Println(queries)

    return queries, nil
}
