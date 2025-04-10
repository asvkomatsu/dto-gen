package config

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
