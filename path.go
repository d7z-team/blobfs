package blobfs

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

func validateTenantID(tenantID string, cfg Config) error {
	if tenantID == "" {
		return errors.New("tenant id must not be empty")
	}
	if len(tenantID) > cfg.MaxTenantLength {
		return fmt.Errorf("tenant id exceeds %d bytes", cfg.MaxTenantLength)
	}
	if tenantID == "." || tenantID == ".." || strings.Contains(tenantID, "\x00") {
		return fmt.Errorf("invalid tenant id %q", tenantID)
	}
	for _, r := range tenantID {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.' {
			continue
		}
		return fmt.Errorf("invalid tenant id %q", tenantID)
	}
	return nil
}

func normalizePath(path string, cfg Config) (string, error) {
	path = strings.ReplaceAll(filepath.ToSlash(path), "\\", "/")
	if path == "" {
		return "", errors.New("invalid object path: path must not be empty")
	}
	if len(path) > cfg.MaxPathLength {
		return "", fmt.Errorf("object path exceeds %d bytes", cfg.MaxPathLength)
	}
	if strings.HasPrefix(path, "/") || (len(path) > 1 && path[1] == ':') {
		return "", fmt.Errorf("invalid object path %q: absolute paths are not allowed", path)
	}
	parts := strings.Split(path, "/")
	clean := make([]string, 0, len(parts))
	for _, item := range parts {
		if item == "" || item == "." || item == ".." || strings.Contains(item, "\x00") {
			return "", fmt.Errorf("invalid object path component %q", item)
		}
		if len(item) > cfg.MaxComponentLength {
			return "", fmt.Errorf("object path component exceeds %d bytes", cfg.MaxComponentLength)
		}
		clean = append(clean, item)
	}
	return strings.Join(clean, "/"), nil
}

func pathBase(path string) string {
	if path == "" {
		return ""
	}
	if i := strings.LastIndex(path, "/"); i >= 0 {
		return path[i+1:]
	}
	return path
}

func parentPath(path string) string {
	if i := strings.LastIndex(path, "/"); i >= 0 {
		return path[:i]
	}
	return ""
}
