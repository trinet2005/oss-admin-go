package madmin

/* trinet */

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestGlobalLifecycleClient(t *testing.T) {
	var putRequest globalLifecyclePutRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/minio/admin/v3/global-lifecycle" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		switch r.Method {
		case http.MethodPut:
			if err := json.NewDecoder(r.Body).Decode(&putRequest); err != nil {
				t.Errorf("decode PUT: %v", err)
			}
			w.WriteHeader(http.StatusNoContent)
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
  "enabled": false,
  "capacityTrigger": 0.8,
  "capacityStop": 0.7,
  "monitorInterval": "5m",
  "rules": [],
  "generation": 0,
  "updatedAt": null,
  "capacityActivatedAt": null
}`))
		case http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	client, err := New(strings.TrimPrefix(server.URL, "http://"), "access", "secret", false)
	if err != nil {
		t.Fatal(err)
	}
	zero := uint64(0)
	cfg := GlobalLifecycleConfig{
		Enabled:         true,
		CapacityTrigger: .8,
		CapacityStop:    .7,
		MonitorInterval: GlobalLifecycleDuration(5 * time.Minute),
		Rules: []GlobalLifecycleRule{{
			ID:         "default",
			Status:     "Enabled",
			Filter:     GlobalLifecycleFilter{Prefix: ""},
			AccessTime: GlobalLifecycleAccessTime{MinimumMinutes: 30},
			Transition: &GlobalLifecycleTransition{StorageClass: "COLD"},
		}},
	}
	if err = client.SetGlobalLifecycle(context.Background(), cfg, &zero); err != nil {
		t.Fatal(err)
	}
	if putRequest.Generation == nil || *putRequest.Generation != 0 {
		t.Fatalf("generation=0 precondition was not encoded: %+v", putRequest)
	}
	if putRequest.MonitorInterval.Duration() != 5*time.Minute || len(putRequest.Rules) != 1 {
		t.Fatalf("unexpected PUT body: %+v", putRequest)
	}

	got, err := client.GetGlobalLifecycle(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Enabled || got.Generation != 0 || got.MonitorInterval.Duration() != 5*time.Minute || got.Rules == nil {
		t.Fatalf("unexpected GET result: %+v", got)
	}
	if err = client.DeleteGlobalLifecycle(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestGlobalLifecycleDurationJSON(t *testing.T) {
	data, err := json.Marshal(GlobalLifecycleDuration(90 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `"1m30s"` {
		t.Fatalf("unexpected duration JSON: %s", data)
	}
	var duration GlobalLifecycleDuration
	if err = json.Unmarshal([]byte(`"2m"`), &duration); err != nil {
		t.Fatal(err)
	}
	if duration.Duration() != 2*time.Minute {
		t.Fatalf("unexpected duration: %v", duration.Duration())
	}
}

func TestDefaultGlobalLifecycleConfig(t *testing.T) {
	cfg := DefaultGlobalLifecycleConfig()
	if cfg.Enabled || cfg.CapacityTrigger != .8 || cfg.CapacityStop != .7 || cfg.MonitorInterval.Duration() != 5*time.Minute || cfg.Rules == nil {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestGlobalLifecycleLiveServer(t *testing.T) {
	endpoint := os.Getenv("OSS_ADMIN_INTEGRATION_ENDPOINT")
	if endpoint == "" {
		t.Skip("OSS_ADMIN_INTEGRATION_ENDPOINT is not set")
	}
	accessKey := os.Getenv("OSS_ADMIN_INTEGRATION_ACCESS_KEY")
	secretKey := os.Getenv("OSS_ADMIN_INTEGRATION_SECRET_KEY")
	coldEndpoint := os.Getenv("OSS_ADMIN_INTEGRATION_COLD_ENDPOINT")
	if accessKey == "" || secretKey == "" || coldEndpoint == "" {
		t.Fatal("integration credentials and cold endpoint must be set")
	}
	coldAccessKey := os.Getenv("OSS_ADMIN_INTEGRATION_COLD_ACCESS_KEY")
	if coldAccessKey == "" {
		coldAccessKey = accessKey
	}
	coldSecretKey := os.Getenv("OSS_ADMIN_INTEGRATION_COLD_SECRET_KEY")
	if coldSecretKey == "" {
		coldSecretKey = secretKey
	}
	coldBucket := os.Getenv("OSS_ADMIN_INTEGRATION_COLD_BUCKET")
	if coldBucket == "" {
		coldBucket = "cold-tier"
	}
	tierName := os.Getenv("OSS_ADMIN_INTEGRATION_TIER_NAME")
	if tierName == "" {
		tierName = "COLD"
	}
	bucketTierName := tierName + "-BUCKET"
	monitorInterval := 5 * time.Second
	if value := os.Getenv("OSS_ADMIN_INTEGRATION_MONITOR_INTERVAL"); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			t.Fatalf("invalid OSS_ADMIN_INTEGRATION_MONITOR_INTERVAL: %v", err)
		}
		monitorInterval = parsed
	}

	client, err := New(endpoint, accessKey, secretKey, false)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tiers, err := client.ListTiers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	foundTiers := make(map[string]bool)
	for _, tier := range tiers {
		foundTiers[tier.Name] = true
	}
	if !foundTiers[tierName] {
		tier, err := NewTierMinIO(tierName, coldEndpoint, coldAccessKey, coldSecretKey, coldBucket)
		if err != nil {
			t.Fatal(err)
		}
		if err = client.AddTier(ctx, tier); err != nil {
			t.Fatal(err)
		}
	}
	if !foundTiers[bucketTierName] {
		tier, err := NewTierMinIO(bucketTierName, coldEndpoint, coldAccessKey, coldSecretKey, coldBucket, MinIOPrefix("bucket-rule"))
		if err != nil {
			t.Fatal(err)
		}
		if err = client.AddTier(ctx, tier); err != nil {
			t.Fatal(err)
		}
	}

	current, err := client.GetGlobalLifecycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	expectedGeneration := current.Generation
	cfg := DefaultGlobalLifecycleConfig()
	cfg.Enabled = true
	cfg.CapacityTrigger = .005
	cfg.CapacityStop = .001
	cfg.MonitorInterval = GlobalLifecycleDuration(monitorInterval)
	cfg.Rules = []GlobalLifecycleRule{{
		ID:         "default",
		Status:     GlobalLifecycleRuleEnabled,
		Filter:     GlobalLifecycleFilter{Prefix: ""},
		AccessTime: GlobalLifecycleAccessTime{MinimumMinutes: 0},
		Transition: &GlobalLifecycleTransition{StorageClass: tierName},
	}}
	if err = client.SetGlobalLifecycle(ctx, cfg, &expectedGeneration); err != nil {
		t.Fatal(err)
	}
	loaded, err := client.GetGlobalLifecycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.Enabled || loaded.Generation != expectedGeneration+1 || loaded.CapacityActivatedAt == nil || len(loaded.Rules) != 1 {
		t.Fatalf("unexpected live configuration: %+v", loaded)
	}
	if err = client.SetGlobalLifecycle(ctx, cfg, &expectedGeneration); err == nil {
		t.Fatal("stale generation update must fail")
	} else if response := ToErrorResponse(err); response.Code != "XMinioAdminGlobalLifecycleConflict" {
		t.Fatalf("unexpected stale generation error: %+v", response)
	}
}

func TestGlobalLifecycleLiveDeleteAndTierReference(t *testing.T) {
	endpoint := os.Getenv("OSS_ADMIN_INTEGRATION_ENDPOINT")
	if endpoint == "" {
		t.Skip("OSS_ADMIN_INTEGRATION_ENDPOINT is not set")
	}
	client, err := New(endpoint, os.Getenv("OSS_ADMIN_INTEGRATION_ACCESS_KEY"), os.Getenv("OSS_ADMIN_INTEGRATION_SECRET_KEY"), false)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tierName := os.Getenv("OSS_ADMIN_INTEGRATION_TIER_NAME")
	if tierName == "" {
		tierName = "COLD"
	}
	before, err := client.GetGlobalLifecycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.RemoveTier(ctx, tierName); err == nil {
		t.Fatal("a tier referenced by global lifecycle must not be removable")
	} else if response := ToErrorResponse(err); response.Code != "XMinioAdminTierReferencedByGlobalLifecycle" {
		t.Fatalf("unexpected tier reference error: %+v", response)
	}
	if err = client.DeleteGlobalLifecycle(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := client.GetGlobalLifecycle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Enabled || after.Generation != before.Generation+1 || len(after.Rules) != 0 || after.CapacityActivatedAt != nil {
		t.Fatalf("unexpected live tombstone: before=%+v after=%+v", before, after)
	}
}

/* trinet */
