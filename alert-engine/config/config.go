package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all alert-engine configuration.
type Config struct {
	Kafka KafkaConfig  `mapstructure:"kafka"`
	Rules []RuleConfig `mapstructure:"rules"`
}

// KafkaConfig holds Kafka consumer settings.
type KafkaConfig struct {
	Brokers       []string     `mapstructure:"brokers"`
	ConsumerGroup string       `mapstructure:"consumer_group"`
	Topics        TopicsConfig `mapstructure:"topics"`
}

// TopicsConfig holds the Kafka topic names used by this service.
type TopicsConfig struct {
	LogsNormalized string `mapstructure:"logs_normalized"`
}

// RuleConfig defines a single alerting rule loaded from config.
type RuleConfig struct {
	Name            string `mapstructure:"name"`
	TenantID        string `mapstructure:"tenant_id"`
	Level           string `mapstructure:"level"`
	MessageContains string `mapstructure:"message_contains"`
	Threshold       int    `mapstructure:"threshold"`
	WindowSecs      int    `mapstructure:"window_seconds"`
	Webhook         string `mapstructure:"webhook"`
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
		v.AddConfigPath("/etc/alert-engine")
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
	v.SetDefault("kafka.brokers", []string{"localhost:9092"})
	v.SetDefault("kafka.consumer_group", "alert-engine")
	v.SetDefault("kafka.topics.logs_normalized", "logs-normalized")
}
