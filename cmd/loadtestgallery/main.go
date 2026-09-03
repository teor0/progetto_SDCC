// Command loadtestgallery drives concurrent load specifically against
// Gallery Service's command and query paths (through the API Gateway),
// independent of Upload/Notification Service. Unlike cmd/loadtest, which
// hammers ONE shared gallery to stress concurrency on a single hotspot,
// this tool grows a pool of many galleries during the measured run, so
// ListGalleries/ListGalleriesByMember get exercised against an actually
// expanding dataset -- the more relevant shape of load for evaluating the
// query side of a CQRS split.
//
// Usage:
//
//	go run ./cmd/loadtestgallery -gateway http://<PUBLIC_IPV4>:8080 \
//	  -moderators 5 -members 50 -duration 60s
//
// Prerequisites for testing this against a scaled Gallery Service:
//  2. gallery-service must not have a static host port mapping while
//     scaled -- same "only one container can bind a host port" issue
//     that hit notification-service earlier. Check docker-compose.yml
//     before running --scale gallery-service=N.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var gatewayURL string

var httpClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     90 * time.Second,
	},
}

type config struct {
	moderators     int
	members        int
	duration       time.Duration
	createInterval time.Duration // how often EACH moderator attempts to create a new gallery
	joinPct        int
	leavePct       int
	getPct         int
	listMembersPct int
	listMyPct      int
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

// galleryPool is the shared, growing set of gallery IDs member workers
// pick from. It starts seeded during setup and keeps growing throughout
// the load phase as moderator workers create more -- deliberately mutable
// concurrent state, guarded by a mutex rather than recreated per read,
// since member workers need to see newly created galleries mid-run.
type galleryPool struct {
	mu  sync.RWMutex
	ids []string
}

func (p *galleryPool) add(id string) {
	p.mu.Lock()
	p.ids = append(p.ids, id)
	p.mu.Unlock()
}

func (p *galleryPool) random(rng *rand.Rand) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.ids) == 0 {
		return "", false
	}
	return p.ids[rng.Intn(len(p.ids))], true
}

func (p *galleryPool) size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.ids)
}

func main() {
	cfg := parseFlags()

	log.Printf("gallery service load test: %d moderators, %d members, %s duration, gateway=%s",
		cfg.moderators, cfg.members, cfg.duration, gatewayURL)
	log.Printf("member workload mix: join=%d%% leave=%d%% get=%d%% listMembers=%d%% listMy=%d%% listAll=%d%%",
		cfg.joinPct, cfg.leavePct, cfg.getPct, cfg.listMembersPct, cfg.listMyPct,
		100-cfg.joinPct-cfg.leavePct-cfg.getPct-cfg.listMembersPct-cfg.listMyPct)
	log.Printf("moderators each attempt CreateGallery every %s", cfg.createInterval)

	if cfg.duration > 50*time.Minute {
		log.Println("WARNING: -duration is close to the 1h JWT TTL -- late failures may be token expiry, not real load-related errors")
	}

	pool := &galleryPool{}

	log.Println("--- setup phase (not measured) ---")

	moderatorTokens := make([]string, cfg.moderators)
	{
		sem := make(chan struct{}, 20)
		var wg sync.WaitGroup
		for i := 0; i < cfg.moderators; i++ {
			wg.Add(1)
			sem <- struct{}{}
			go func(i int) {
				defer wg.Done()
				defer func() { <-sem }()
				token := mustRegister("ROLE_MODERATOR")
				moderatorTokens[i] = token
				// Seed one gallery per moderator up front, so member
				// workers have something to Join/Get/ListMembers against
				// from the very first tick of the load phase instead of
				// racing an empty pool.
				g := mustCreateGallerySetup(token)
				pool.add(g.ID)
			}(i)
		}
		wg.Wait()
	}
	log.Printf("registered %d moderators, seeded %d galleries", cfg.moderators, pool.size())

	memberTokens := make([]string, cfg.members)
	{
		sem := make(chan struct{}, 20)
		var wg sync.WaitGroup
		for i := 0; i < cfg.members; i++ {
			wg.Add(1)
			sem <- struct{}{}
			go func(i int) {
				defer wg.Done()
				defer func() { <-sem }()
				memberTokens[i] = mustRegister("ROLE_USER")
			}(i)
		}
		wg.Wait()
	}
	log.Printf("registered %d members", cfg.members)

	memberOps := buildMemberOps(cfg)

	log.Println("--- load phase ---")

	var wg sync.WaitGroup
	resultsCh := make(chan []opResult, cfg.moderators+cfg.members)
	stop := time.Now().Add(cfg.duration)
	start := time.Now()

	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Printf("... %d requests completed so far, %d galleries in pool", totalRequests.Load(), pool.size())
			case <-progressDone:
				return
			}
		}
	}()

	for _, token := range moderatorTokens {
		wg.Add(1)
		go func(token string) {
			defer wg.Done()
			resultsCh <- runModeratorWorker(token, pool, cfg.createInterval, stop)
		}(token)
	}
	for _, token := range memberTokens {
		wg.Add(1)
		go func(token string) {
			defer wg.Done()
			resultsCh <- runMemberWorker(token, pool, memberOps, stop)
		}(token)
	}

	wg.Wait()
	close(progressDone)
	close(resultsCh)
	actualDuration := time.Since(start)

	var all []opResult
	for r := range resultsCh {
		all = append(all, r...)
	}

	report(all, actualDuration)
	log.Printf("final gallery pool size: %d", pool.size())
}

func parseFlags() config {
	gateway := flag.String("gateway", "http://localhost:8080", "API gateway base URL")
	moderators := flag.Int("moderators", 5, "number of moderator accounts, each periodically creating new galleries")
	members := flag.Int("members", 50, "number of member accounts driving join/leave/read load")
	duration := flag.Duration("duration", 60*time.Second, "how long to run the measured load phase")
	createInterval := flag.Duration("create-interval", 3*time.Second, "how often EACH moderator attempts to create a new gallery")
	joinPct := flag.Int("join-pct", 15, "percent of member operations that are JoinGallery")
	leavePct := flag.Int("leave-pct", 10, "percent of member operations that are LeaveGallery")
	getPct := flag.Int("get-pct", 20, "percent of member operations that are GetGallery")
	listMembersPct := flag.Int("list-members-pct", 15, "percent of member operations that are ListMembers")
	listMyPct := flag.Int("list-my-pct", 20, "percent of member operations that are ListGalleries(my_galleries=true); remainder is ListGalleries(all)")

	flag.Parse()

	sum := *joinPct + *leavePct + *getPct + *listMembersPct + *listMyPct
	if sum > 100 || *joinPct < 0 || *leavePct < 0 || *getPct < 0 || *listMembersPct < 0 || *listMyPct < 0 {
		log.Fatal("join/leave/get/list-members/list-my percentages must each be >= 0 and sum to <= 100")
	}
	if *moderators < 1 || *members < 1 {
		log.Fatal("-moderators and -members must each be at least 1")
	}

	gatewayURL = *gateway

	return config{
		moderators:     *moderators,
		members:        *members,
		duration:       *duration,
		createInterval: *createInterval,
		joinPct:        *joinPct,
		leavePct:       *leavePct,
		getPct:         *getPct,
		listMembersPct: *listMembersPct,
		listMyPct:      *listMyPct,
	}
}

// memberOp is one weighted operation a member worker might perform.
// Weight is a percentage point count (they're built to sum to 100 by
// buildMemberOps), and pickMemberOp does a straightforward weighted pick
// over them -- a small table-driven approach rather than a cascade of
// if/else percentage bands, since there are six operations here instead
// of cmd/loadtest's two or three.
type memberOp struct {
	name   string
	weight int
	run    func(token string, pool *galleryPool, rng *rand.Rand) opResult
}

func buildMemberOps(cfg config) []memberOp {
	listAllPct := 100 - cfg.joinPct - cfg.leavePct - cfg.getPct - cfg.listMembersPct - cfg.listMyPct
	return []memberOp{
		{name: "joinGallery", weight: cfg.joinPct, run: timedJoinGallery},
		{name: "leaveGallery", weight: cfg.leavePct, run: timedLeaveGallery},
		{name: "getGallery", weight: cfg.getPct, run: timedGetGallery},
		{name: "listMembers", weight: cfg.listMembersPct, run: timedListMembers},
		{name: "listMyGalleries", weight: cfg.listMyPct, run: func(token string, pool *galleryPool, rng *rand.Rand) opResult {
			return timedListGalleries(token, true)
		}},
		{name: "listAllGalleries", weight: listAllPct, run: func(token string, pool *galleryPool, rng *rand.Rand) opResult {
			return timedListGalleries(token, false)
		}},
	}
}

func pickMemberOp(ops []memberOp, rng *rand.Rand) memberOp {
	total := 0
	for _, o := range ops {
		total += o.weight
	}
	r := rng.Intn(total)
	for _, o := range ops {
		if r < o.weight {
			return o
		}
		r -= o.weight
	}
	return ops[len(ops)-1] // unreachable given the weights sum correctly, but a safe fallback
}

func runMemberWorker(token string, pool *galleryPool, ops []memberOp, stop time.Time) []opResult {
	var results []opResult
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for time.Now().Before(stop) {
		op := pickMemberOp(ops, rng)
		res := op.run(token, pool, rng)
		results = append(results, res)
		totalRequests.Add(1)
	}
	return results
}

// runModeratorWorker calls CreateGallery on a fixed interval rather than
// as fast as possible -- moderators creating content is inherently rarer
// than members browsing/joining/leaving it, and an unthrottled creation
// loop would flood the pool with galleries far faster than any real
// usage pattern, skewing what the read-side latencies actually mean.
func runModeratorWorker(token string, pool *galleryPool, interval time.Duration, stop time.Time) []opResult {
	var results []opResult
	for {
		time.Sleep(interval)
		if time.Now().After(stop) {
			return results
		}
		res := timedCreateGallery(token, pool)
		results = append(results, res)
		totalRequests.Add(1)
	}
}

func timedCreateGallery(token string, pool *galleryPool) opResult {
	start := time.Now()

	name := fmt.Sprintf("loadtest-gallery-%d-%d", time.Now().UnixNano(), rand.Int())
	body, _ := json.Marshal(map[string]string{
		"name":        name,
		"description": "created by cmd/loadtestgallery",
	})

	req, err := http.NewRequest(http.MethodPost, gatewayURL+"/photogallery/galleries", bytes.NewReader(body))
	if err != nil {
		return opResult{op: "createGallery", latency: time.Since(start), ok: false}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := httpClient.Do(req)
	if err != nil {
		return opResult{op: "createGallery", latency: time.Since(start), ok: false}
	}
	defer resp.Body.Close()

	ok := resp.StatusCode == http.StatusOK
	if ok {
		var g galleryResponse
		if json.NewDecoder(resp.Body).Decode(&g) == nil && g.ID != "" {
			pool.add(g.ID)
		} else {
			ok = false // decoded 200 but got garbage back -- treat as a failure, don't silently skip adding to the pool
		}
	} else {
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
	}

	return opResult{op: "createGallery", latency: time.Since(start), ok: ok}
}

// timedJoinGallery and timedLeaveGallery pick a RANDOM gallery from the
// pool with no membership bookkeeping -- both operations are idempotent
// server-side (JoinGallery's OnConflict{DoNothing}, LeaveGallery's
// no-op-if-absent Delete), so joining something you're already in, or
// leaving something you were never in, both just succeed. That's what
// makes uncoordinated random selection across many concurrent workers
// safe here, rather than a source of false failures.
func timedJoinGallery(token string, pool *galleryPool, rng *rand.Rand) opResult {
	id, ok := pool.random(rng)
	if !ok {
		return opResult{op: "joinGallery", latency: 0, ok: false}
	}
	start := time.Now()

	req, err := http.NewRequest(http.MethodPost, gatewayURL+"/photogallery/galleries/"+id+"/members", nil)
	if err != nil {
		return opResult{op: "joinGallery", latency: time.Since(start), ok: false}
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := httpClient.Do(req)
	if err != nil {
		return opResult{op: "joinGallery", latency: time.Since(start), ok: false}
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) //nolint:errcheck

	return opResult{op: "joinGallery", latency: time.Since(start), ok: resp.StatusCode == http.StatusOK}
}

func timedLeaveGallery(token string, pool *galleryPool, rng *rand.Rand) opResult {
	id, ok := pool.random(rng)
	if !ok {
		return opResult{op: "leaveGallery", latency: 0, ok: false}
	}
	start := time.Now()

	req, err := http.NewRequest(http.MethodDelete, gatewayURL+"/photogallery/galleries/"+id+"/members", nil)
	if err != nil {
		return opResult{op: "leaveGallery", latency: time.Since(start), ok: false}
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := httpClient.Do(req)
	if err != nil {
		return opResult{op: "leaveGallery", latency: time.Since(start), ok: false}
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) //nolint:errcheck

	return opResult{op: "leaveGallery", latency: time.Since(start), ok: resp.StatusCode == http.StatusOK}
}

func timedGetGallery(token string, pool *galleryPool, rng *rand.Rand) opResult {
	id, ok := pool.random(rng)
	if !ok {
		return opResult{op: "getGallery", latency: 0, ok: false}
	}
	start := time.Now()

	req, err := http.NewRequest(http.MethodGet, gatewayURL+"/photogallery/galleries/"+id, nil)
	if err != nil {
		return opResult{op: "getGallery", latency: time.Since(start), ok: false}
	}
	req.Header.Set("Authorization", "Bearer "+token) // not required (GetGallery is public), sent anyway for consistency

	resp, err := httpClient.Do(req)
	if err != nil {
		return opResult{op: "getGallery", latency: time.Since(start), ok: false}
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) //nolint:errcheck

	return opResult{op: "getGallery", latency: time.Since(start), ok: resp.StatusCode == http.StatusOK}
}

func timedListMembers(token string, pool *galleryPool, rng *rand.Rand) opResult {
	id, ok := pool.random(rng)
	if !ok {
		return opResult{op: "listMembers", latency: 0, ok: false}
	}
	start := time.Now()

	req, err := http.NewRequest(http.MethodGet, gatewayURL+"/photogallery/galleries/"+id+"/members", nil)
	if err != nil {
		return opResult{op: "listMembers", latency: time.Since(start), ok: false}
	}
	req.Header.Set("Authorization", "Bearer "+token) // required -- ListMembers is not a public method

	resp, err := httpClient.Do(req)
	if err != nil {
		return opResult{op: "listMembers", latency: time.Since(start), ok: false}
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) //nolint:errcheck

	return opResult{op: "listMembers", latency: time.Since(start), ok: resp.StatusCode == http.StatusOK}
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
	io.Copy(io.Discard, resp.Body) //nolint:errcheck

	return opResult{op: op, latency: time.Since(start), ok: resp.StatusCode == http.StatusOK}
}

func mustRegister(role string) string {
	email := fmt.Sprintf("loadtestgallery-%d-%d@example.com", time.Now().UnixNano(), rand.Int())
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

// mustCreateGallerySetup is used only during the unmeasured setup phase,
// to seed one gallery per moderator before the load phase starts.
func mustCreateGallerySetup(moderatorToken string) galleryResponse {
	body, _ := json.Marshal(map[string]string{
		"name":        fmt.Sprintf("loadtest-gallery-seed-%d", time.Now().UnixNano()),
		"description": "seed gallery created by cmd/loadtestgallery setup",
	})

	req, err := http.NewRequest(http.MethodPost, gatewayURL+"/photogallery/galleries", bytes.NewReader(body))
	if err != nil {
		log.Fatalf("create seed gallery: build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+moderatorToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		log.Fatalf("create seed gallery: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		log.Fatalf("create seed gallery: status=%d body=%s", resp.StatusCode, b)
	}

	var g galleryResponse
	if err := json.NewDecoder(resp.Body).Decode(&g); err != nil {
		log.Fatalf("create seed gallery: decode response: %v", err)
	}
	return g
}

// --- reporting (same shape as cmd/loadtest, duplicated rather than
// shared -- this tool is meant to be runnable standalone without pulling
// in anything from the upload-focused tool) ---

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
	fmt.Println("=== Gallery Service Load Test Results ===")
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
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

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
