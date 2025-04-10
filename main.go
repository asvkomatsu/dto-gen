package main

import (
	config2 "dto-gen/config"
	metadata2 "dto-gen/metadata"
	"dto-gen/metago"
	"dto-gen/metapy"
	"dto-gen/pgsql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func isValidDirectoryName(name string) bool {
	pattern := `^[a-z0-9]+$`
	re := regexp.MustCompile(pattern)
	return re.MatchString(name)
}

func readMetadata(config config2.Config) (*metadata2.Metadata, error) {
	if config.ConnInfo.DBMS == "PostgreSQL" {
		return pgsql.ReadPostgresMetadata(config)
	}

	// no metadata read
	return nil, fmt.Errorf("unsupported DMBS: %s", config.ConnInfo.DBMS)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: dto-gen <folder-with-db.json>")
		os.Exit(1)
	}

	folder := os.Args[1]
	parts := strings.Split(os.Args[1], "/")
	if !isValidDirectoryName(parts[len(parts)-1]) {
		fmt.Println("Invalid Directory Name. Should be lowercase alphanumeric only!")
		os.Exit(1)
	}

	// Build path to config file
	configFile := filepath.Join(folder, "db.json")
	_, err := os.Stat(configFile)
	if os.IsNotExist(err) {
		fmt.Println("db config file does not exist")
		os.Exit(1)
	}

	// Read config file
	data, err := os.ReadFile(configFile)
	if err != nil {
		fmt.Println("Error reading file: ", err)
		os.Exit(1)
	}

	// Parse json file
	var config config2.Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		fmt.Println("Error parsing config file: ", err)
		os.Exit(1)
	}

	fmt.Println(config)

	// Reading database metadata
	metadata, err := readMetadata(config)
	if err != nil {
		fmt.Println("Error reading metadata: ", err)
		os.Exit(1)
	}
	// metadata.print()

	// Reading custom_queries.conf
	customQueries, err := config2.ReadCustomQueries(folder)
	if err != nil {
		fmt.Println("Error reading custom queries: ", err)
		os.Exit(1)
	}

	if config.Language == "go" {
		err = metago.WriteGolang(&(config.ConnInfo), folder, metadata, customQueries)
		if err != nil {
			fmt.Println("Error writing go source code: ", err)
			os.Exit(1)
		}
	} else if config.Language == "python" {
		err = metapy.WritePython(&(config.ConnInfo), folder, metadata, customQueries)
		if err != nil {
			fmt.Println("Error writing python source code: ", err)
		}
	} else {
		fmt.Println("Unknown language " + config.Language)
		os.Exit(1)
	}
}
