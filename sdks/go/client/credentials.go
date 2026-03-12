package client

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// TokenPath returns the file path where a token for the given host and module would be stored.
// Default path: ~/.spacetimedb/go_client_tokens/{sanitized_host}_{module_name}
func TokenPath(host, moduleName string) string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "."
	}
	sanitized := sanitizeComponent(host) + "_" + sanitizeComponent(moduleName)
	return filepath.Join(home, ".spacetimedb", "go_client_tokens", sanitized)
}

// SaveToken writes the token to a local file with restrictive permissions (0600).
func SaveToken(host, moduleName, token string) error {
	path := TokenPath(host, moduleName)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("credentials: create directory %s: %w", dir, err)
	}
	if err := os.WriteFile(path, []byte(token), 0600); err != nil {
		return fmt.Errorf("credentials: write token to %s: %w", path, err)
	}
	return nil
}

// LoadToken reads a previously saved token. Returns an empty string and no error if no token exists.
func LoadToken(host, moduleName string) (string, error) {
	path := TokenPath(host, moduleName)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("credentials: read token from %s: %w", path, err)
	}
	return string(data), nil
}

// sanitizeComponent replaces characters that are not safe for filenames.
func sanitizeComponent(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}
