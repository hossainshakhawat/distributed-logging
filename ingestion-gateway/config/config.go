package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all ingestion-gateway configuration.
type Config struct {
	ListenAddr   string            `mapstructure:"listen_addr"`
	RateLimitRPS int               `mapstructure:"rate_limit_rps"`
	APIKeys      map[string]string `mapstructure:"api_keys"`
	Kafka        KafkaConfig       `mapstructure:"kafka"`
}

// KafkaConfig holds Kafka connection settings.
type KafkaConfig struct {
	Brokers []string     `mapstructure:"brokers"`
	Topics  TopicsConfig `mapstructure:"topics"`
}

// TopicsConfig holds the Kafka topic names used by this service.
type TopicsConfig struct {
	LogsRaw string `mapstructure:"logs_raw"`
}

// Load reads config.yml and applies environment variable overrides.
// Nested keys use underscore separation in env vars
// (e.g. KAFKA_BROKERS overrides kafka.brokers).
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
		v.AddConfigPath("/etc/ingestion-gateway")
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
	v.SetDefault("listen_addr", ":8080")
	v.SetDefault("rate_limit_rps", 1000)
	v.SetDefault("api_keys", map[string]string{})
	v.SetDefault("kafka.brokers", []string{"localhost:9092"})
	v.SetDefault("kafka.topics.logs_raw", "logs-raw")
}
