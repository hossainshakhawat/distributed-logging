package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all log-agent configuration.
type Config struct {
	GatewayURL        string   `mapstructure:"gateway_url"`
	TenantID          string   `mapstructure:"tenant_id"`
	ServiceName       string   `mapstructure:"service_name"`
	Environment       string   `mapstructure:"environment"`
	LogPaths          []string `mapstructure:"log_paths"`
	BatchSize         int      `mapstructure:"batch_size"`
	FlushIntervalSecs int      `mapstructure:"flush_interval_seconds"`
	BufferDir         string   `mapstructure:"buffer_dir"`
}

// Load reads config.yml (searched in working dir then /etc/log-agent),
// applies environment variable overrides, and returns the resolved Config.
// Environment variables are uppercased, with "." replaced by "_"
// (e.g. GATEWAY_URL overrides gateway_url).
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
		v.AddConfigPath("/etc/log-agent")
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
	v.SetDefault("gateway_url", "http://localhost:8080")
	v.SetDefault("tenant_id", "default")
	v.SetDefault("service_name", "unknown")
	v.SetDefault("environment", "production")
	v.SetDefault("log_paths", []string{"/var/log/app/app.log"})
	v.SetDefault("batch_size", 100)
	v.SetDefault("flush_interval_seconds", 2)
	v.SetDefault("buffer_dir", "/tmp/log-agent-buffer")
}
