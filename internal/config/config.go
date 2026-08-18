package config

import (
	"os"
	"strconv"
	"time"
)

// String returns the environment variable named by key, or fallback if the
// variable is unset or empty.
func String(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Int returns the environment variable named by key parsed as an int, or
// fallback if the variable is unset, empty, or not a valid int.
func Int(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

// Duration returns the environment variable named by key parsed with
// time.ParseDuration (e.g. "5s"), or fallback if the variable is unset,
// empty, or not a valid duration.
func Duration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
