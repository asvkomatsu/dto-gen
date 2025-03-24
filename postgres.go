package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
)

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
	var query = "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ("
	for i := 0; i < len(schemas); i++ {
		if i > 0 {
			query += ", "
		}
		query += "'" + schemas[i] + "'"
	}
	query += ")"
	query += " AND table_type IN ('BASE TABLE', 'VIEW')"
	fmt.Printf("Query: %s\n", query)

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
	var query = "SELECT table_schema, table_name, column_name, data_type, is_nullable, column_default " +
		"FROM information_schema.columns WHERE table_schema IN ("
	for i := 0; i < len(schemas); i++ {
		if i > 0 {
			query += ", "
		}
		query += "'" + schemas[i] + "'"
	}
	query += ")"
	fmt.Printf("Query: %s\n", query)

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
		columnMap[key] = append(columnMap[key], column)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over columns list rows: %w", err)
	}

	return columnMap, nil
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

	return &Metadata{
		Database: config.ConnInfo.Database,
		Tables:   tables,
	}, nil
}

// consultas para ler constraints das colunas
select kcu.table_schema, kcu.table_name, kcu.column_name, kcu.constraint_name, tc.constraint_type
from information_schema.key_column_usage kcu
inner join information_schema.table_constraints tc on kcu.constraint_name = tc.constraint_name
where kcu.table_schema in ('public');

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
AND tc.table_schema in ('public');
