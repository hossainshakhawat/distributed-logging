package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all query-api configuration.
type Config struct {
	ListenAddr       string            `mapstructure:"listen_addr"`
	HotRetentionDays int               `mapstructure:"hot_retention_days"`
	APIKeys          map[string]string `mapstructure:"api_keys"`
	OpenSearch       OpenSearchConfig  `mapstructure:"opensearch"`
}

// OpenSearchConfig holds OpenSearch client settings.
type OpenSearchConfig struct {
	Addresses []string `mapstructure:"addresses"`
	Username  string   `mapstructure:"username"`
	Password  string   `mapstructure:"password"`
}

// Load reads config.yml and applies environment variable overrides.
func Load() (*Config, error) {
	v := viper.New()
	setDefaults(v)

	v.SetConfigName("config")
	v.SetConfigType("yaml")

	if cf := os.Getenv("CONFIG_FILE"); cf != "" {
		v.SetConfigFile(cf)
	} else {
		v.AddConfigPath(".")
		v.AddConfigPath("./config")
		v.AddConfigPath("/etc/query-api")
	}

	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))

	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if !errors.As(err, &notFound) {
			return nil, fmt.Errorf("load config: %w", err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	return &cfg, nil
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("listen_addr", ":8081")
	v.SetDefault("hot_retention_days", 30)
	v.SetDefault("api_keys", map[string]string{})
	v.SetDefault("opensearch.addresses", []string{"http://localhost:9200"})
	v.SetDefault("opensearch.username", "admin")
	v.SetDefault("opensearch.password", "Admin@12345")
}
