package blobfs

import (
	"errors"
	"io/fs"
	"strings"
	"testing"
)

func TestValidateTenantIDBoundaries(t *testing.T) {
	cfg := testConfig()
	cfg.MaxTenantLength = 4
	valid := []string{"a", "A1", "x_y", "x-y", "x.y"}
	for _, tenantID := range valid {
		if err := validateTenantID(tenantID, cfg); err != nil {
			t.Fatalf("valid tenant %q rejected: %v", tenantID, err)
		}
	}
	invalid := []string{"", ".", "..", "abcde", "bad/tenant", "bad tenant", "bad\x00tenant"}
	for _, tenantID := range invalid {
		if err := validateTenantID(tenantID, cfg); err == nil {
			t.Fatalf("invalid tenant %q accepted", tenantID)
		}
	}
}

func TestNormalizePathBoundaries(t *testing.T) {
	cfg := testConfig()
	cfg.MaxPathLength = 8
	cfg.MaxComponentLength = 4
	if got, err := normalizePath("a/b", cfg); err != nil || got != "a/b" {
		t.Fatalf("normalize valid path = %q, %v", got, err)
	}
	invalid := []string{
		"",
		"/abs",
		`C:\abs`,
		"a//b",
		"a/./b",
		"a/../b",
		"abcde",
		strings.Repeat("a", cfg.MaxPathLength+1),
		"a/\x00",
	}
	for _, path := range invalid {
		if _, err := normalizePath(path, cfg); err == nil {
			t.Fatalf("invalid path %q accepted", path)
		}
	}
}

func TestTenantFSRejectsInvalidNames(t *testing.T) {
	store := openTestStore(t)
	if _, err := store.TenantFS("tenant-a").Open("../escape"); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("tenant fs invalid path = %v, want invalid", err)
	}
	if _, err := store.TenantFS("bad/tenant").Open("."); err == nil {
		t.Fatal("tenant fs invalid tenant should fail")
	}
}
