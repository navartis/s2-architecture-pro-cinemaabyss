// Command proxy is a Strangler Fig API gateway for CinemaAbyss.
//
// It stands in front of the monolith and the services that have already been
// extracted, and decides per request where the traffic goes. The share of
// /api/movies requests that reach the movies service is controlled by
// MOVIES_MIGRATION_PERCENT, so the migration can be advanced without a deploy
// and rolled back without a code change.
package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// upstream is a named reverse proxy to one backend.
type upstream struct {
	name  string
	proxy *httputil.ReverseProxy
}

// newUpstream builds a reverse proxy for rawURL. A backend failure is turned
// into 502 here instead of reaching the client as a dropped connection.
func newUpstream(name, rawURL string) upstream {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		log.Fatalf("invalid %s url %q: %v", name, rawURL, err)
	}

	p := httputil.NewSingleHostReverseProxy(u)
	p.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("upstream %s failed for %s %s: %v", name, r.Method, r.URL.Path, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"upstream unavailable","upstream":"` + name + `"}`))
	}

	log.Printf("upstream %s -> %s", name, u)
	return upstream{name: name, proxy: p}
}

// gateway routes requests between the monolith and the extracted services.
type gateway struct {
	monolith upstream
	movies   upstream
	events   upstream

	gradual       bool // GRADUAL_MIGRATION, master switch for the feature flag
	moviesPercent int  // MOVIES_MIGRATION_PERCENT, 0..100
}

// useMoviesService is the feature flag itself: false means "everything goes to
// the monolith", true means "this particular request goes to the movies
// service". Values 0 and 100 are deterministic; anything in between is random
// per request, which is what makes gradual migration measurable.
func (g *gateway) useMoviesService() bool {
	if !g.gradual || g.moviesPercent <= 0 {
		return false
	}
	if g.moviesPercent >= 100 {
		return true
	}
	return rand.Intn(100) < g.moviesPercent
}

// forward passes the request to up and leaves a line in the log naming the
// chosen backend, so the migration can be shown on real traffic.
func (g *gateway) forward(w http.ResponseWriter, r *http.Request, up upstream, reason string) {
	log.Printf("route %s %s -> %s (%s)", r.Method, r.URL.Path, up.name, reason)
	up.proxy.ServeHTTP(w, r)
}

func (g *gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/health":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("Strangler Fig Proxy is healthy"))

	case strings.HasPrefix(r.URL.Path, "/api/events"):
		g.forward(w, r, g.events, "events domain, always extracted")

	case strings.HasPrefix(r.URL.Path, "/api/movies"):
		if g.useMoviesService() {
			g.forward(w, r, g.movies, "feature flag, movies service")
		} else {
			g.forward(w, r, g.monolith, "feature flag, monolith")
		}

	default:
		g.forward(w, r, g.monolith, "domain not migrated yet")
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envPercent reads a percentage and keeps it inside 0..100. A malformed value
// falls back to 0 (full monolith), which is the safe side of the flag.
func envPercent(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		log.Printf("invalid %s=%q, using %d: %v", key, raw, fallback, err)
		return fallback
	}
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return v
}

func envBool(key string, fallback bool) bool {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		log.Printf("invalid %s=%q, using %t: %v", key, raw, fallback, err)
		return fallback
	}
	return v
}

func main() {
	g := &gateway{
		monolith:      newUpstream("monolith", envOr("MONOLITH_URL", "http://localhost:8080")),
		movies:        newUpstream("movies-service", envOr("MOVIES_SERVICE_URL", "http://localhost:8081")),
		events:        newUpstream("events-service", envOr("EVENTS_SERVICE_URL", "http://localhost:8082")),
		gradual:       envBool("GRADUAL_MIGRATION", false),
		moviesPercent: envPercent("MOVIES_MIGRATION_PERCENT", 0),
	}

	addr := ":" + envOr("PORT", "8000")
	log.Printf("strangler fig proxy listening on %s (gradual_migration=%t, movies_migration_percent=%d)",
		addr, g.gradual, g.moviesPercent)

	srv := &http.Server{
		Addr:              addr,
		Handler:           g,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Fatal(srv.ListenAndServe())
}
