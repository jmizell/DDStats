package ddstats

import (
	"os"
	"testing"
)

func TestConfig_FromEnv(t *testing.T) {

	vars := [][2]string{
		{EnvNamespace, "testNamespace"},
		{EnvHost, "testHost"},
		{EnvAPIKey, "testAPIKey"},
		{EnvFlushIntervalSeconds, "1"},
		{EnvWorkerCount, "2"},
		{EnvWorkerBuffer, "3"},
		{EnvMetricBuffer, "4"},
		{EnvMaxErrorCount, "5"},
		{EnvTags, "tag:1,tag:2"},
	}
	for i := range vars {
		if err := os.Setenv(vars[i][0], vars[i][1]); err != nil {
			t.Fatalf("could not set environment %s", vars[i])
		}
	}

	cfg := NewConfig().FromEnv()

	if cfg.Namespace != vars[0][1] {
		t.Fatalf("expected Namespace to be %s, have %s", vars[0][1], cfg.Namespace)
	}
	if cfg.Host != vars[1][1] {
		t.Fatalf("expected Host to be %s, have %s", vars[1][1], cfg.Host)
	}
	if cfg.APIKey != vars[2][1] {
		t.Fatalf("expected APIKey to be %s, have %s", vars[2][1], cfg.APIKey)
	}
	if cfg.FlushIntervalSeconds != 1.0 {
		t.Fatalf("expected FlushIntervalSeconds to be %f, have %f", 1.0, cfg.FlushIntervalSeconds)
	}
	if cfg.WorkerCount != 2 {
		t.Fatalf("expected WorkerCount to be %d, have %d", 2, cfg.WorkerCount)
	}
	if cfg.WorkerBuffer != 3 {
		t.Fatalf("expected WorkerBuffer to be %d, have %d", 3, cfg.WorkerBuffer)
	}
	if cfg.MetricBuffer != 4 {
		t.Fatalf("expected MetricBuffer to be %d, have %d", 4, cfg.MetricBuffer)
	}
	if cfg.MaxErrors != 5 {
		t.Fatalf("expected MaxErrors to be %d, have %d", 5, cfg.MaxErrors)
	}

	if len(cfg.Tags) != 2 {
		t.Fatalf("expected to have %d tags, have %d", 2, len(cfg.Tags))
	}
	if cfg.Tags[0] != "tag:1" {
		t.Fatalf("expected Tags[0] to be %s, have %s", "tag:1", cfg.Tags[0])
	}
	if cfg.Tags[1] != "tag:2" {
		t.Fatalf("expected Tags[1] to be %s, have %s", "tag:2", cfg.Tags[1])
	}
}

func TestConfig_WithAPIKey(t *testing.T) {
	cfg := NewConfig().WithAPIKey("test key")

	if cfg.APIKey != "test key" {
		t.Fatalf("expected api key to be %s, have %s", "test key", cfg.APIKey)
	}
}
