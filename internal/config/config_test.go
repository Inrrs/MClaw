package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load('') failed: %v", err)
	}
	if cfg.Server.Port != "8900" {
		t.Errorf("default port = %q, want 8900", cfg.Server.Port)
	}
	if cfg.Auth.WebUIUser != "admin" {
		t.Errorf("default webui user = %q, want admin", cfg.Auth.WebUIUser)
	}
}

func TestLoadFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	data := `{"server":{"port":"9999"},"auth":{"api_key":"test-key"}}`
	os.WriteFile(path, []byte(data), 0644)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Server.Port != "9999" {
		t.Errorf("port = %q, want 9999", cfg.Server.Port)
	}
	if cfg.Auth.APIKey != "test-key" {
		t.Errorf("api_key = %q, want test-key", cfg.Auth.APIKey)
	}
}

func TestLoadEnvOverride(t *testing.T) {
	t.Setenv("SERVER_PORT", "7777")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Server.Port != "7777" {
		t.Errorf("port = %q, want 7777 (env override)", cfg.Server.Port)
	}
}

func TestConfigPaths(t *testing.T) {
	cfg := &Config{DataDir: "/tmp/test"}
	if cfg.ModelMappingPath() != filepath.Join("/tmp/test", "model_mapping.json") {
		t.Errorf("ModelMappingPath = %q", cfg.ModelMappingPath())
	}
	if cfg.DBPath() != filepath.Join("/tmp/test", "mclaw.db") {
		t.Errorf("DBPath = %q", cfg.DBPath())
	}
}

func TestConfigDefaultDataDir(t *testing.T) {
	cfg := &Config{}
	if cfg.dataDir() != "data" {
		t.Errorf("default dataDir = %q, want data", cfg.dataDir())
	}
}
