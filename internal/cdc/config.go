package cdc

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	defaultMySQLFlavor  = "mysql"
	defaultMySQLCharset = "utf8mb4"
	defaultServerID     = 2001
	defaultReadTimeout  = "30s"
	defaultESIndexPrefx = "bm"
)

type Config struct {
	MySQLAddr         string
	MySQLUser         string
	MySQLPassword     string
	MySQLFlavor       string
	MySQLCharset      string
	MySQLReadTimeout  string
	MySQLServerID     uint32
	IncludeTableRegex []string

	ESAddr        string
	ESIndexPrefix string
}

func LoadConfigFromEnv() (*Config, error) {
	serverID, err := parseUint32Env("CDC_MYSQL_SERVER_ID", defaultServerID)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		MySQLAddr:         strings.TrimSpace(os.Getenv("CDC_MYSQL_ADDR")),
		MySQLUser:         strings.TrimSpace(os.Getenv("CDC_MYSQL_USER")),
		MySQLPassword:     os.Getenv("CDC_MYSQL_PASSWORD"),
		MySQLFlavor:       getenvDefault("CDC_MYSQL_FLAVOR", defaultMySQLFlavor),
		MySQLCharset:      getenvDefault("CDC_MYSQL_CHARSET", defaultMySQLCharset),
		MySQLReadTimeout:  getenvDefault("CDC_MYSQL_READ_TIMEOUT", defaultReadTimeout),
		MySQLServerID:     serverID,
		IncludeTableRegex: splitCSV(getenvDefault("CDC_INCLUDE_TABLE_REGEX", "big_market_01\\..*,big_market_02\\..*")),
		ESAddr:            strings.TrimRight(strings.TrimSpace(os.Getenv("CDC_ES_ADDR")), "/"),
		ESIndexPrefix:     strings.ToLower(getenvDefault("CDC_ES_INDEX_PREFIX", defaultESIndexPrefx)),
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.MySQLAddr == "" {
		return fmt.Errorf("CDC_MYSQL_ADDR is required")
	}
	if c.MySQLUser == "" {
		return fmt.Errorf("CDC_MYSQL_USER is required")
	}
	if c.ESAddr == "" {
		return fmt.Errorf("CDC_ES_ADDR is required")
	}
	if len(c.IncludeTableRegex) == 0 {
		return fmt.Errorf("CDC_INCLUDE_TABLE_REGEX must contain at least one regex")
	}
	if c.MySQLServerID == 0 {
		return fmt.Errorf("CDC_MYSQL_SERVER_ID must be greater than 0")
	}
	return nil
}

func (c *Config) LogicalIndexName(tableName string) string {
	logicalTableName := trimShardSuffix(tableName)
	prefix := strings.TrimSpace(c.ESIndexPrefix)
	if prefix == "" {
		return strings.ToLower(logicalTableName)
	}
	return strings.ToLower(prefix + "_" + logicalTableName)
}

func getenvDefault(key string, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue
	}
	return value
}

func parseUint32Env(key string, defaultValue uint32) (uint32, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return defaultValue, nil
	}

	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}

	return uint32(parsed), nil
}

func splitCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}

	items := strings.Split(value, ",")
	result := make([]string, 0, len(items))
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}

	return result
}
