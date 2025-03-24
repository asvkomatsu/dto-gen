package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type ConnectionInfo struct {
	DBMS     string   `json:"dbms"`
	Host     string   `json:"host"`
	Port     int      `json:"port"`
	Username string   `json:"username"`
	Password string   `json:"password"`
	Database string   `json:"database"`
	Schemas  []string `json:"schemas"`
}

type Config struct {
	Language string         `json:"language"`
	ConnInfo ConnectionInfo `json:"connection"`
}

func readMetadata(config Config) (*Metadata, error) {
	if config.ConnInfo.DBMS == "PostgreSQL" {
		return readPostgresMetadata(config)
	}

	// no metadata read
	return nil, fmt.Errorf("unsupported DMBS: %s", config.ConnInfo.DBMS)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: dto-gen <folder-with-db.json>")
		os.Exit(1)
	}

	// Build path to config file
	folder := os.Args[1]
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
	var config Config
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
	metadata.print()

	if config.Language == "go" {
		err = write_golang(folder, metadata)
		if err != nil {
			fmt.Println("Error writing go source code: ", err)
			os.Exit(1)
		}
	} else {
		fmt.Println("Unknown language " + config.Language)
		os.Exit(1)
	}
}
