package main

import (
    "context"
    "fmt"
    "github.com/jackc/pgx/v5"
)

var PostgreSQLTypes = map[string]string{
    "bigint":                      "int64",
    "bigserial":                   "int64",
    "bit":                         "bool",
    "boolean":                     "bool",
    "bytea":                       "[]byte",
    "character":                   "rune",
    "character varying":           "string",
    "date":                        "time.Time",
    "double precision":            "float32",
    "integer":                     "int",
    "money":                       "float64",
    "numeric":                     "float64",
    "real":                        "float32",
    "serial":                      "int",
    "smallint":                    "int16",
    "smallserial":                 "int16",
    "text":                        "string",
    "timestamp without time zone": "time.Time",
}

type PgConstraints struct {
    Schema string
    Table  string
    Column string
    Name   string
    Type   string
}

type PgFkInfo struct {
    ConstraintName   string
    FkSchema         string
    FkTable          string
    FkColumn         string
    ReferencedSchema string
    ReferencedTable  string
    ReferencedColumn string
}

type PgAutoIncrementInfo struct {
    Schema        string
    Table         string
    Column        string
    IncrementType string
}

func connectToPostgres(connInfo ConnectionInfo) (*pgx.Conn, error) {
    var dburl = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
        connInfo.Host, connInfo.Port, connInfo.Username, connInfo.Password, connInfo.Database)
    fmt.Printf("Connection String: %s\n", dburl)

    conn, err := pgx.Connect(context.Background(), dburl)
    if err != nil {
        return nil, fmt.Errorf("error connecting to postgres: %w", err)
    }
    return conn, nil
}

func readPgTables(conn *pgx.Conn, schemas []string) ([]Table, error) {
    var query = `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_schema IN (
	`
    for i := 0; i < len(schemas); i++ {
        if i > 0 {
            query += ", "
        }
        query += "'" + schemas[i] + "'"
    }
    query += ") AND table_type IN ('BASE TABLE', 'VIEW')"
    // fmt.Printf("Query: %s\n", query)

    rows, err := conn.Query(context.Background(), query)
    if err != nil {
        return nil, fmt.Errorf("failed to query table list: %w", err)
    }
    defer rows.Close()

    var tables []Table
    for rows.Next() {
        var table Table
        err := rows.Scan(&table.Schema, &table.Name)
        if err != nil {
            return nil, fmt.Errorf("failed to scan table list row: %w", err)
        }
        tables = append(tables, table)
    }

    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("error iterating over table list rows: %w", err)
    }

    return tables, nil
}

func readPgColumns(conn *pgx.Conn, schemas []string) (map[string][]Column, error) {
    var query = `
		SELECT ordinal_position, table_schema, table_name, column_name, data_type, is_nullable, column_default
		FROM information_schema.columns WHERE table_schema IN (
	`
    for i := 0; i < len(schemas); i++ {
        if i > 0 {
            query += ", "
        }
        query += "'" + schemas[i] + "'"
    }
    query += ")"
    query += " ORDER BY ordinal_position"
    // fmt.Printf("Query: %s\n", query)

    rows, err := conn.Query(context.Background(), query)
    if err != nil {
        return nil, fmt.Errorf("failed to query columns list: %w", err)
    }
    defer rows.Close()

    var columnMap = make(map[string][]Column)
    for rows.Next() {
        var column Column
        var tableName string
        var tableSchema string
        var nullable string
        err := rows.Scan(
            &column.Ordinal,
            &tableSchema,
            &tableName,
            &column.Name,
            &column.Datatype,
            &nullable,
            &column.DefaultValue)
        if err != nil {
            return nil, fmt.Errorf("failed to scan columns list row: %w", err)
        }
        var key = fmt.Sprintf("%s.%s", tableSchema, tableName)
        column.Nullable = nullable == "YES"
        column.IsPrimaryKey = false
        column.IsAutoIncrement = false
        columnMap[key] = append(columnMap[key], column)
    }

    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("error iterating over columns list rows: %w", err)
    }

    return columnMap, nil
}

func readPgConstraints(conn *pgx.Conn, schemas []string) ([]PgConstraints, error) {
    var query = `
		SELECT kcu.table_schema, kcu.table_name, kcu.column_name, kcu.constraint_name, tc.constraint_type
		FROM information_schema.key_column_usage kcu
			INNER JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name
		WHERE kcu.table_schema IN (
	`
    for i := 0; i < len(schemas); i++ {
        if i > 0 {
            query += ", "
        }
        query += "'" + schemas[i] + "'"
    }
    query += ")"
    // fmt.Printf("Query: %s\n", query)

    rows, err := conn.Query(context.Background(), query)
    if err != nil {
        return nil, fmt.Errorf("failed to query constraint list: %w", err)
    }
    defer rows.Close()

    var constraints = make([]PgConstraints, 0)
    for rows.Next() {
        var constraint PgConstraints
        err := rows.Scan(
            &constraint.Schema,
            &constraint.Table,
            &constraint.Column,
            &constraint.Name,
            &constraint.Type)
        if err != nil {
            return nil, fmt.Errorf("failed to scan constraint list row: %w", err)
        }
        constraints = append(constraints, constraint)
    }

    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("error iterating over constraint list rows: %w", err)
    }

    return constraints, nil
}

func readFkInfo(conn *pgx.Conn, schemas []string) ([]PgFkInfo, error) {
    var query = `
		SELECT
			tc.constraint_name,
			tc.table_schema AS fk_schema,
			tc.table_name AS fk_table,
			kcu.column_name AS fk_column,
			ccu.table_schema AS referenced_schema,
			ccu.table_name AS referenced_table,
			ccu.column_name AS referenced_column
		FROM information_schema.table_constraints AS tc
				 JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
				 JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
		WHERE tc.constraint_type = 'FOREIGN KEY'
		  AND tc.table_schema in (
	`
    for i := 0; i < len(schemas); i++ {
        if i > 0 {
            query += ", "
        }
        query += "'" + schemas[i] + "'"
    }
    query += ")"
    // fmt.Printf("Query: %s\n", query)

    rows, err := conn.Query(context.Background(), query)
    if err != nil {
        return nil, fmt.Errorf("failed to query fk list: %w", err)
    }
    defer rows.Close()

    var fkInfos = make([]PgFkInfo, 0)
    for rows.Next() {
        var fkInfo PgFkInfo
        err := rows.Scan(
            &fkInfo.ConstraintName,
            &fkInfo.FkSchema,
            &fkInfo.FkTable,
            &fkInfo.FkColumn,
            &fkInfo.ReferencedSchema,
            &fkInfo.ReferencedTable,
            &fkInfo.ReferencedColumn)
        if err != nil {
            return nil, fmt.Errorf("failed to scan fk list row: %w", err)
        }
        fkInfos = append(fkInfos, fkInfo)
    }

    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("error iterating over fk list rows: %w", err)
    }

    return fkInfos, nil
}

func readAutoIncrementInfo(conn *pgx.Conn, schemas []string) ([]PgAutoIncrementInfo, error) {
    var query = `
		SELECT
			c.table_schema,
			c.table_name,
			c.column_name,
			CASE
				WHEN pg_get_serial_sequence(format('%I.%I', c.table_schema, c.table_name), c.column_name) IS NOT NULL THEN 'SERIAL'
				WHEN c.is_identity = 'YES' THEN 'IDENTITY'
				ELSE 'NOT AUTO INCREMENT'
			END AS auto_increment_type
		FROM information_schema.columns c
		WHERE c.table_schema IN (
	`
    for i := 0; i < len(schemas); i++ {
        if i > 0 {
            query += ", "
        }
        query += "'" + schemas[i] + "'"
    }
    query += ")"
    // fmt.Printf("Query: %s\n", query)

    rows, err := conn.Query(context.Background(), query)
    if err != nil {
        return nil, fmt.Errorf("failed to query auto increment info list: %w", err)
    }
    defer rows.Close()

    var aiInfos = make([]PgAutoIncrementInfo, 0)
    for rows.Next() {
        var aiInfo PgAutoIncrementInfo
        err := rows.Scan(
            &aiInfo.Schema,
            &aiInfo.Table,
            &aiInfo.Column,
            &aiInfo.IncrementType)
        if err != nil {
            return nil, fmt.Errorf("failed to scan auto increment info list row: %w", err)
        }
        aiInfos = append(aiInfos, aiInfo)
    }

    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("error iterating over auto increment info list rows: %w", err)
    }

    return aiInfos, nil
}

func readPostgresMetadata(config Config) (*Metadata, error) {
    conn, err := connectToPostgres(config.ConnInfo)
    if err != nil {
        return nil, err
    }
    defer conn.Close(context.Background())

    // read tables
    tables, err := readPgTables(conn, config.ConnInfo.Schemas)
    if err != nil {
        return nil, fmt.Errorf("failed to read table list: %w", err)
    }

    // read columns
    columnsMap, err := readPgColumns(conn, config.ConnInfo.Schemas)
    if err != nil {
        return nil, fmt.Errorf("failed to read columns list: %w", err)
    }

    // associate tables with columns
    for i := 0; i < len(tables); i++ {
        var key = fmt.Sprintf("%s.%s", tables[i].Schema, tables[i].Name)
        columns, exists := columnsMap[key]
        if exists {
            tables[i].Columns = columns
        } else {
        }
    }

    // read constraints
    constraints, err := readPgConstraints(conn, config.ConnInfo.Schemas)
    if err != nil {
        return nil, fmt.Errorf("failed to read constraint list: %w", err)
    }

    // read foreign key references
    pgFkInfos, err := readFkInfo(conn, config.ConnInfo.Schemas)
    if err != nil {
        return nil, fmt.Errorf("failed to read fk list: %w", err)
    }

    // read auto increment info
    pgAutoIncrementInfos, err := readAutoIncrementInfo(conn, config.ConnInfo.Schemas)
    if err != nil {
        return nil, fmt.Errorf("failed to read auto increment list: %w", err)
    }

    // mark primary keys and foreign keys
    for i := range tables {
        cols := tables[i].Columns
        for j := range cols {
            for k := range constraints {
                if constraints[k].Schema == tables[i].Schema && constraints[k].Table == tables[i].Name {
                    if constraints[k].Column == cols[j].Name && constraints[k].Type == "PRIMARY KEY" {
                        cols[j].IsPrimaryKey = true
                    } else if constraints[k].Column == cols[j].Name && constraints[k].Type == "FOREIGN KEY" {
                        var idx = -1
                        for l := range pgFkInfos {
                            if pgFkInfos[l].FkSchema == tables[i].Schema &&
                                pgFkInfos[l].FkTable == tables[i].Name &&
                                pgFkInfos[l].FkColumn == cols[j].Name {
                                idx = l
                            }
                        }
                        if idx > -1 {
                            var fkTarget = ForeignKeyTarget{
                                Schema: pgFkInfos[idx].ReferencedSchema,
                                Table:  pgFkInfos[idx].ReferencedTable,
                                Column: pgFkInfos[idx].ReferencedColumn,
                            }
                            cols[j].FkTarget = &fkTarget
                        }
                    }
                }
            }

            for k := range pgAutoIncrementInfos {
                if pgAutoIncrementInfos[k].Schema == tables[i].Schema &&
                    pgAutoIncrementInfos[k].Table == tables[i].Name &&
                    pgAutoIncrementInfos[k].Column == cols[j].Name &&
                    pgAutoIncrementInfos[k].IncrementType != "NOT AUTO INCREMENT" {
                    cols[j].IsAutoIncrement = true
                }
            }
        }
    }

    return &Metadata{
        Database: config.ConnInfo.Database,
        Tables:   tables,
    }, nil
}
