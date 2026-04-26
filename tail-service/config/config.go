package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all tail-service configuration.
type Config struct {
	ListenAddr        string      `mapstructure:"listen_addr"`
	MaxActiveSessions int         `mapstructure:"max_active_sessions"`
	Kafka             KafkaConfig `mapstructure:"kafka"`
}

// KafkaConfig holds Kafka consumer settings.
type KafkaConfig struct {
	Brokers       []string `mapstructure:"brokers"`
	ConsumerGroup string   `mapstructure:"consumer_group"`
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
		v.AddConfigPath("/etc/tail-service")
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
	v.SetDefault("listen_addr", ":8082")
	v.SetDefault("max_active_sessions", 500)
	v.SetDefault("kafka.brokers", []string{"localhost:9092"})
	v.SetDefault("kafka.consumer_group", "tail-service")
}
