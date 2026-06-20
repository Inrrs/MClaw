package api

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultModelMapping(t *testing.T) {
	m := defaultModelMapping()
	if len(m) == 0 {
		t.Fatal("defaultModelMapping() returned empty map")
	}
	if m["gpt-4o"] != "mimo-v2.5-pro" {
		t.Errorf("gpt-4o mapping = %q, want mimo-v2.5-pro", m["gpt-4o"])
	}
	if m["gpt-4o-mini"] != "mimo-v2.5" {
		t.Errorf("gpt-4o-mini mapping = %q, want mimo-v2.5", m["gpt-4o-mini"])
	}
}

func TestLoadAndSaveModelMapping(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mapping.json")

	// 初始加载应生成默认映射
	LoadModelMapping(path)
	if len(modelMapping) == 0 {
		t.Fatal("LoadModelMapping did not load default mapping")
	}

	// 保存新映射
	newMapping := map[string]string{"test-model": "mimo-test"}
	if err := SaveModelMapping(path, newMapping); err != nil {
		t.Fatalf("SaveModelMapping failed: %v", err)
	}

	// 重新加载
	LoadModelMapping(path)
	got := ApplyModelMapping("test-model")
	if got != "mimo-test" {
		t.Errorf("ApplyModelMapping(test-model) = %q, want mimo-test", got)
	}

	// 未映射的模型应原样返回
	got = ApplyModelMapping("unknown-model")
	if got != "unknown-model" {
		t.Errorf("ApplyModelMapping(unknown-model) = %q, want unknown-model", got)
	}
}

func TestLoadModelMappingInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")
	os.WriteFile(path, []byte("not json"), 0644)

	LoadModelMapping(path)
	// 应回退到默认映射
	if len(modelMapping) == 0 {
		t.Fatal("LoadModelMapping did not fall back to default on bad JSON")
	}
}
