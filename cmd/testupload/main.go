// Use this main to test against a running deployment of
// API Gateway and create reports with per-operation latency
// distribution and error rate.
// Usage:
//
//	go run ./cmd/testupload -gateway http://<PUBLIC_IPV4>:8080 -users 50 -duration 30s
//
// Example profiles:
//
//	read-heavy (default):
//	  go run ./cmd/testupload -gateway http://<IP>:8080 -users 100 -duration 60s
//
// Chaos injection (automatically trip and recover the circuit breaker):
//
// use -upload-pct to specify the percent of requests that are photo uploads
// use -list-my-pct to specify the percent of requests that are ListGalleries(my_galleries=true)
//
//	go run ./cmd/testupload -gateway http://<IP>:8080 -users 30 -duration 90s \
//	  -upload-pct 60 -list-my-pct 20 \
//	  -chaos-after 20s -chaos-duration 20s \
//	  -chaos-ssh-host ec2-user@<IP> -chaos-ssh-key ./labsuser.pem
//
//	use go run ./cmd/testupload -users 5 -duration 15s -chaos-after 5s -chaos-duration 5s to check ssh connection
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"mime/multipart"
	"net/http"
	"os/exec"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// gatewayURL is set once from flags in main
var gatewayURL string

// httpClient is shared across every request the load generator makes.
// The explicit transport matters: net/http's zero-value transport (what
// http.DefaultClient uses) caps idle connections per host at 2, which
// would cause a bottleneck
var httpClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     90 * time.Second,
	},
}

var fakePhotoBytes = bytes.Repeat([]byte("x"), 2048)

type config struct {
	gatewayURL string
	users      int
	duration   time.Duration
	uploadPct  int
	listMyPct  int
}

// chaosConfig controls the automatic fault injection
type chaosConfig struct {
	enabled    bool
	after      time.Duration
	duration   time.Duration
	service    string
	sshHost    string
	sshKey     string
	composeDir string // remote directory containing docker-compose.yml
}

type opResult struct {
	op      string
	latency time.Duration
	ok      bool
}

type tokenResponse struct {
	Token string `json:"token"`
}

type galleryResponse struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

var totalRequests atomic.Int64

func main() {
	cfg, chaos := parseFlags()

	log.Printf("scalability test: %d concurrent users, %s duration, gateway=%s",
		cfg.users, cfg.duration, cfg.gatewayURL)
	log.Printf("workload mix: %d%% listMyGalleries, %d%% listAllGalleries, %d%% uploadPhoto",
		cfg.listMyPct, 100-cfg.listMyPct-cfg.uploadPct, cfg.uploadPct)
	if chaos.enabled {
		log.Printf("chaos injection enabled: stop %s at t+%s, restart at t+%s (via %s)",
			chaos.service, chaos.after, chaos.after+chaos.duration, chaos.sshHost)
		if chaos.after+chaos.duration > cfg.duration {
			log.Printf("WARNING: -chaos-after + -chaos-duration extends past -duration; " +
				"the tool will keep running past the load phase until the chaos restart completes")
		}
	}

	log.Println("--- setup phase ---")

	moderatorToken := mustRegister("ROLE_MODERATOR")
	gallery := mustCreateGallery(moderatorToken, "Scalability Test Gallery")
	log.Printf("created gallery %s (%q)", gallery.ID, gallery.Name)

	tokens := setupUsers(cfg, gallery.ID)
	log.Printf("registered and joined %d users", len(tokens))

	if cfg.duration > 50*time.Minute {
		log.Println("WARNING: -duration is close to the JWT TTL")
	}

	log.Println("--- load phase ---")

	var wg sync.WaitGroup
	resultsCh := make(chan []opResult, cfg.users)
	stop := time.Now().Add(cfg.duration)
	start := time.Now()

	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Printf("... %d requests completed so far", totalRequests.Load())
			case <-progressDone:
				return
			}
		}
	}()

	var chaosWG sync.WaitGroup
	var chaosStopOffset, chaosRestartOffset time.Duration

	if chaos.enabled {
		chaosWG.Add(1)
		go func() {
			defer chaosWG.Done()
			runChaos(chaos, start, &chaosStopOffset, &chaosRestartOffset)
		}()
	}

	for _, token := range tokens {
		wg.Add(1)
		go func(token string) {
			defer wg.Done()
			resultsCh <- runWorker(cfg, token, gallery.ID, stop)
		}(token)
	}

	wg.Wait()
	close(progressDone)
	close(resultsCh)
	actualDuration := time.Since(start)

	// Wait for any in-progress chaos restart
	chaosWG.Wait()

	var all []opResult
	for r := range resultsCh {
		all = append(all, r...)
	}

	report(all, actualDuration)

	if chaos.enabled {
		fmt.Printf("chaos injection: stopped %s at t+%s, restarted at t+%s\n",
			chaos.service, chaosStopOffset.Round(time.Millisecond), chaosRestartOffset.Round(time.Millisecond))
		fmt.Println("cross-reference these offsets against `docker compose logs upload-service | grep CircuitBreaker` ")
	}
}

func parseFlags() (config, chaosConfig) {
	gateway := flag.String("gateway", "http://localhost:8080", "API gateway base URL")
	users := flag.Int("users", 50, "number of concurrent virtual users")
	duration := flag.Duration("duration", 30*time.Second, "how long to run the measured load phase")
	uploadPct := flag.Int("upload-pct", 10, "percent of requests that are photo uploads")
	listMyPct := flag.Int("list-my-pct", 60, "percent of requests that are ListGalleries(my_galleries=true)")

	chaosAfter := flag.Duration("chaos-after", 0, "if > 0, stop -chaos-service this long after the load phase starts")
	chaosDuration := flag.Duration("chaos-duration", 20*time.Second, "how long to keep -chaos-service stopped before restarting it")
	chaosService := flag.String("chaos-service", "gallery-service", "docker compose service name to stop/start for the chaos injection")
	chaosSSHHost := flag.String("chaos-ssh-host", "", "user@host for SSH'ing into the deployment to run docker compose")
	chaosSSHKey := flag.String("chaos-ssh-key", "", "path to the SSH private key for -chaos-ssh-host")
	chaosComposeDir := flag.String("chaos-compose-dir", "~/photogallery", "remote directory containing docker-compose.yml")

	flag.Parse()

	if *uploadPct < 0 || *listMyPct < 0 || *uploadPct+*listMyPct > 100 {
		log.Fatal("upload-pct and list-my-pct must each be >= 0 and sum to <= 100")
	}
	if *users < 2 {
		log.Fatal("-users must be at least 2")
	}

	gatewayURL = *gateway

	chaos := chaosConfig{
		enabled:    *chaosAfter > 0,
		after:      *chaosAfter,
		duration:   *chaosDuration,
		service:    *chaosService,
		sshHost:    *chaosSSHHost,
		sshKey:     *chaosSSHKey,
		composeDir: *chaosComposeDir,
	}
	if chaos.enabled && chaos.sshHost == "" {
		log.Fatal("-chaos-after > 0 requires -chaos-ssh-host (e.g. ec2-user@<PUBLIC_IPV4>)")
	}

	return config{
		gatewayURL: *gateway,
		users:      *users,
		duration:   *duration,
		uploadPct:  *uploadPct,
		listMyPct:  *listMyPct,
	}, chaos
}

func runChaos(cfg chaosConfig, loadPhaseStart time.Time, stopOffset, restartOffset *time.Duration) {
	time.Sleep(cfg.after)
	*stopOffset = time.Since(loadPhaseStart)
	log.Printf("=== CHAOS: stopping %s (t+%s) ===", cfg.service, stopOffset.Round(time.Millisecond))
	if err := runRemoteCompose(cfg, "stop", cfg.service); err != nil {
		log.Printf("CHAOS: failed to stop %s: %v -- chaos injection aborted, %s was never stopped", cfg.service, err, cfg.service)
		return
	}

	time.Sleep(cfg.duration)
	*restartOffset = time.Since(loadPhaseStart)
	log.Printf("=== CHAOS: restarting %s (t+%s) ===", cfg.service, restartOffset.Round(time.Millisecond))
	if err := runRemoteCompose(cfg, "start", cfg.service); err != nil {
		log.Printf("CHAOS: failed to restart %s: %v -- you will need to restart it manually (docker compose start %s)", cfg.service, err, cfg.service)
	}
}

func runRemoteCompose(cfg chaosConfig, action, service string) error {
	remoteCmd := fmt.Sprintf("cd %s && docker compose %s %s", cfg.composeDir, action, service)

	args := []string{"-o", "BatchMode=yes"}
	if cfg.sshKey != "" {
		args = append(args, "-i", cfg.sshKey)
	}
	args = append(args, cfg.sshHost, remoteCmd)

	cmd := exec.Command("ssh", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ssh %q: %w\noutput: %s", remoteCmd, err, out)
	}
	log.Printf("chaos: %s -> %s", remoteCmd, strings.TrimSpace(string(out)))
	return nil
}

// setupUsers registers cfg.users accounts
func setupUsers(cfg config, galleryID string) []string {
	tokens := make([]string, cfg.users)
	sem := make(chan struct{}, 20)
	var wg sync.WaitGroup

	for i := 0; i < cfg.users; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			token := mustRegister("ROLE_USER")
			mustJoinGallery(token, galleryID)
			tokens[i] = token
		}(i)
	}
	wg.Wait()
	return tokens
}

// runWorker repeatedly performs a random operation
func runWorker(cfg config, token, galleryID string, stop time.Time) []opResult {
	var results []opResult
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for time.Now().Before(stop) {
		roll := rng.Intn(100)

		var res opResult
		switch {
		case roll < cfg.listMyPct:
			res = timedListGalleries(token, true)
		case roll < cfg.listMyPct+cfg.uploadPct:
			res = timedUploadPhoto(token, galleryID)
		default:
			res = timedListGalleries(token, false)
		}

		results = append(results, res)
		totalRequests.Add(1)
	}

	return results
}

func timedListGalleries(token string, mine bool) opResult {
	op := "listAllGalleries"
	url := gatewayURL + "/photogallery/galleries"
	if mine {
		op = "listMyGalleries"
		url += "?my_galleries=true"
	}

	start := time.Now()

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return opResult{op: op, latency: time.Since(start), ok: false}
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := httpClient.Do(req)
	if err != nil {
		return opResult{op: op, latency: time.Since(start), ok: false}
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	return opResult{op: op, latency: time.Since(start), ok: resp.StatusCode == http.StatusOK}
}

func timedUploadPhoto(token, galleryID string) opResult {
	start := time.Now()

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, err := w.CreateFormFile("photo", "loadtest.jpg")
	if err == nil {
		_, err = fw.Write(fakePhotoBytes)
	}
	if err == nil {
		err = w.WriteField("galleryId", galleryID)
	}
	if err == nil {
		err = w.Close()
	}
	if err != nil {
		return opResult{op: "uploadPhoto", latency: time.Since(start), ok: false}
	}

	req, err := http.NewRequest(http.MethodPost, gatewayURL+"/api/uploads", &buf)
	if err != nil {
		return opResult{op: "uploadPhoto", latency: time.Since(start), ok: false}
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := httpClient.Do(req)
	if err != nil {
		return opResult{op: "uploadPhoto", latency: time.Since(start), ok: false}
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) //nolint:errcheck

	// internal/handlers/upload.go collapses every failure to HTTP 500
	// regardless of the underlying gRPC code (circuit-open, not-a-member,
	// storage error, etc. all look identical here) -- this tool can only
	// report pass/fail for uploads, not distinguish *why* one failed.
	// Cross-reference service logs during the run if you need that detail
	// -- which is exactly what the chaos-injection offsets printed at the
	// end are for.
	return opResult{op: "uploadPhoto", latency: time.Since(start), ok: resp.StatusCode == http.StatusOK}
}

func mustRegister(role string) string {
	email := fmt.Sprintf("loadtest-%d-%d@example.com", time.Now().UnixNano(), rand.Int())
	body, _ := json.Marshal(map[string]string{
		"email":    email,
		"password": "password123",
		"role":     role,
	})

	req, err := http.NewRequest(http.MethodPost, gatewayURL+"/photogallery/auth/register", bytes.NewReader(body))
	if err != nil {
		log.Fatalf("register: build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		log.Fatalf("register: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		log.Fatalf("register: status=%d body=%s", resp.StatusCode, b)
	}

	var tr tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tr); err != nil {
		log.Fatalf("register: decode response: %v", err)
	}
	return tr.Token
}

func mustCreateGallery(moderatorToken, name string) galleryResponse {
	body, _ := json.Marshal(map[string]string{
		"name":        name,
		"description": "created by cmd/loadtest",
	})

	req, err := http.NewRequest(http.MethodPost, gatewayURL+"/photogallery/galleries", bytes.NewReader(body))
	if err != nil {
		log.Fatalf("create gallery: build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+moderatorToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		log.Fatalf("create gallery: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		log.Fatalf("create gallery: status=%d body=%s", resp.StatusCode, b)
	}

	var g galleryResponse
	if err := json.NewDecoder(resp.Body).Decode(&g); err != nil {
		log.Fatalf("create gallery: decode response: %v", err)
	}
	return g
}

func mustJoinGallery(token, galleryID string) {
	req, err := http.NewRequest(http.MethodPost, gatewayURL+"/photogallery/galleries/"+galleryID+"/members", nil)
	if err != nil {
		log.Fatalf("join gallery: build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := httpClient.Do(req)
	if err != nil {
		log.Fatalf("join gallery: %v", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		log.Fatalf("join gallery: status=%d", resp.StatusCode)
	}
}

type opStats struct {
	op                            string
	count, errors                 int
	min, mean, p50, p95, p99, max time.Duration
}

func report(all []opResult, actualDuration time.Duration) {
	grouped := make(map[string][]opResult)
	for _, r := range all {
		grouped[r.op] = append(grouped[r.op], r)
	}

	var opNames []string
	for op := range grouped {
		opNames = append(opNames, op)
	}
	sort.Strings(opNames)

	fmt.Println()
	fmt.Println("=== Scalability Test Results ===")
	fmt.Printf("total requests: %d, actual load-phase duration: %s\n\n", len(all), actualDuration)

	for _, op := range opNames {
		printStats(computeStats(op, grouped[op]), actualDuration)
	}
}

func computeStats(op string, results []opResult) opStats {
	latencies := make([]time.Duration, len(results))
	var errors int
	var sum time.Duration

	for i, r := range results {
		latencies[i] = r.latency
		sum += r.latency
		if !r.ok {
			errors++
		}
	}
	slices.Sort(latencies)

	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		idx := int(p * float64(len(latencies)-1))
		return latencies[idx]
	}

	var mean, min, max time.Duration
	if len(results) > 0 {
		mean = sum / time.Duration(len(results))
	}
	if len(latencies) > 0 {
		min = latencies[0]
		max = latencies[len(latencies)-1]
	}

	return opStats{
		op:     op,
		count:  len(results),
		errors: errors,
		min:    min,
		mean:   mean,
		p50:    pct(0.50),
		p95:    pct(0.95),
		p99:    pct(0.99),
		max:    max,
	}
}

func printStats(s opStats, duration time.Duration) {
	errRate := 0.0
	if s.count > 0 {
		errRate = float64(s.errors) / float64(s.count) * 100
	}
	throughput := float64(s.count) / duration.Seconds()

	fmt.Printf("%-18s requests=%-7d errors=%-6d (%.1f%%) throughput=%.1f req/s\n",
		s.op, s.count, s.errors, errRate, throughput)
	fmt.Printf("%-18s min=%-8s mean=%-8s p50=%-8s p95=%-8s p99=%-8s max=%s\n\n",
		"", s.min, s.mean, s.p50, s.p95, s.p99, s.max)
}
