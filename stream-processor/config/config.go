package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all stream-processor configuration.
type Config struct {
	Kafka        KafkaConfig      `mapstructure:"kafka"`
	OpenSearch   OpenSearchConfig `mapstructure:"opensearch"`
	S3           S3Config         `mapstructure:"s3"`
	Redis        RedisConfig      `mapstructure:"redis"`
	Indexer      IndexerConfig    `mapstructure:"indexer"`
	Archiver     ArchiverConfig   `mapstructure:"archiver"`
	DedupTTLSecs int              `mapstructure:"dedup_ttl_seconds"`
}

// KafkaConfig holds Kafka consumer/producer settings.
type KafkaConfig struct {
	Brokers       []string `mapstructure:"brokers"`
	ConsumerGroup string   `mapstructure:"consumer_group"`
}

// OpenSearchConfig holds OpenSearch client settings.
type OpenSearchConfig struct {
	Addresses []string `mapstructure:"addresses"`
	Username  string   `mapstructure:"username"`
	Password  string   `mapstructure:"password"`
}

// S3Config holds object-storage settings.
type S3Config struct {
	Bucket   string `mapstructure:"bucket"`
	Region   string `mapstructure:"region"`
	Endpoint string `mapstructure:"endpoint"` // empty = real AWS; set for MinIO/LocalStack
}

// RedisConfig holds Redis connection settings.
type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

// IndexerConfig holds OpenSearch bulk-indexer tuning.
type IndexerConfig struct {
	BatchSize         int `mapstructure:"batch_size"`
	FlushIntervalSecs int `mapstructure:"flush_interval_seconds"`
}

// ArchiverConfig holds S3 archiver tuning.
type ArchiverConfig struct {
	BatchSize         int `mapstructure:"batch_size"`
	FlushIntervalSecs int `mapstructure:"flush_interval_seconds"`
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
		v.AddConfigPath("/etc/stream-processor")
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
	v.SetDefault("kafka.consumer_group", "stream-processor")

	v.SetDefault("opensearch.addresses", []string{"http://localhost:9200"})
	v.SetDefault("opensearch.username", "admin")
	v.SetDefault("opensearch.password", "Admin@12345")

	v.SetDefault("s3.bucket", "distributed-logs")
	v.SetDefault("s3.region", "us-east-1")
	v.SetDefault("s3.endpoint", "")

	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)

	v.SetDefault("indexer.batch_size", 500)
	v.SetDefault("indexer.flush_interval_seconds", 5)

	v.SetDefault("archiver.batch_size", 1000)
	v.SetDefault("archiver.flush_interval_seconds", 30)

	v.SetDefault("dedup_ttl_seconds", 300)
}
