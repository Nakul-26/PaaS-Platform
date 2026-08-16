package config

import "os"

// String returns the environment variable named by key, or fallback if the
// variable is unset or empty.
func String(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
