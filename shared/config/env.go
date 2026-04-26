package config

import (
	"os"
)

// Getenv returns the environment variable value or a fallback default.
func Getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
