package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDiscover(t *testing.T) {
	t.Run("fetches and caches the discovery document", func(t *testing.T) {
		ClearDiscoveryCache()
		hits := 0
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits++
			issuer := "http://" + r.Host + "/realms/mcpwarp"
			_ = json.NewEncoder(w).Encode(map[string]string{
				"issuer":                        issuer,
				"device_authorization_endpoint": issuer + "/device",
				"token_endpoint":                issuer + "/token",
			})
		}))
		defer srv.Close()

		issuer := srv.URL + "/realms/mcpwarp"
		cfg, err := Discover(context.Background(), issuer, srv.Client())
		if err != nil {
			t.Fatal(err)
		}
		if cfg.TokenEndpoint == "" || cfg.DeviceAuthorizationEndpoint == "" {
			t.Fatal("missing endpoints")
		}

		if _, err := Discover(context.Background(), issuer, srv.Client()); err != nil {
			t.Fatal(err)
		}
		if hits != 1 {
			t.Fatalf("expected 1 hit (cached), got %d", hits)
		}
	})

	t.Run("rejects an issuer mismatch", func(t *testing.T) {
		ClearDiscoveryCache()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]string{
				"issuer":                        "https://wrong/realms/mcpwarp",
				"device_authorization_endpoint": "https://x/device",
				"token_endpoint":                "https://x/token",
			})
		}))
		defer srv.Close()

		_, err := Discover(context.Background(), srv.URL+"/realms/mcpwarp", srv.Client())
		if err == nil {
			t.Fatal("expected an error")
		}
		if _, ok := err.(*DiscoveryError); !ok {
			t.Fatalf("expected *DiscoveryError, got %T", err)
		}
	})

	t.Run("rejects a document missing required endpoints", func(t *testing.T) {
		ClearDiscoveryCache()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			issuer := "http://" + r.Host + "/realms/mcpwarp"
			_ = json.NewEncoder(w).Encode(map[string]string{"issuer": issuer})
		}))
		defer srv.Close()

		_, err := Discover(context.Background(), srv.URL+"/realms/mcpwarp", srv.Client())
		if err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("surfaces a non-2xx response", func(t *testing.T) {
		ClearDiscoveryCache()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()

		_, err := Discover(context.Background(), srv.URL+"/realms/mcpwarp", srv.Client())
		if err == nil {
			t.Fatal("expected an error")
		}
	})

	t.Run("surfaces a network error", func(t *testing.T) {
		ClearDiscoveryCache()
		_, err := Discover(context.Background(), "http://127.0.0.1:1", nil)
		if err == nil {
			t.Fatal("expected an error")
		}
	})
}
