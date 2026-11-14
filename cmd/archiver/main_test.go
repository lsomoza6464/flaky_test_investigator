package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

// fakeGitHub is an httptest server serving canned runs, jobs and logs, and counting requests per path.
type fakeGitHub struct {
	mu             sync.Mutex
	runs           []workflowRun
	jobs           map[string][]job // keyed by "runID/attempt"
	logs           map[int64]string // job ID -> log text
	expired        map[int64]bool   // job IDs whose log returns 410
	rateLimitNext  bool             // reject the next API request as rate-limited
	requests       map[string]int   // URL path -> count, API and blob servers
	createdQueries []string         // the created= value of every runs listing
	apiAuth        string           // Authorization header of the last API request
	blobAuth       string           // Authorization header of the last blob request
	blobURL        string
}

// newFakeGitHub starts a fake server and returns it with an archiver pointed at it and a temp archive dir.
func newFakeGitHub(t *testing.T) (*fakeGitHub, *archiver) {
	t.Helper()
	f := &fakeGitHub{
		jobs:     map[string][]job{},
		logs:     map[int64]string{},
		expired:  map[int64]bool{},
		requests: map[string]int{},
	}

	api := http.NewServeMux()
	api.HandleFunc("GET /repos/{owner}/{repo}/actions/runs", f.serveRuns)
	api.HandleFunc("GET /repos/{owner}/{repo}/actions/runs/{run}/attempts/{attempt}/jobs", f.serveJobs)
	api.HandleFunc("GET /repos/{owner}/{repo}/actions/jobs/{job}/logs", f.serveLogRedirect)
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.requests[r.URL.Path]++
		f.apiAuth = r.Header.Get("Authorization")
		if f.rateLimitNext {
			f.rateLimitNext = false
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(30*time.Second).Unix(), 10))
			w.WriteHeader(http.StatusForbidden)
			return
		}
		api.ServeHTTP(w, r)
	}))
	t.Cleanup(apiServer.Close)

	blobServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.requests[r.URL.Path]++
		f.blobAuth = r.Header.Get("Authorization")
		id, _ := strconv.ParseInt(filepath.Base(r.URL.Path), 10, 64)
		io.WriteString(w, f.logs[id])
	}))
	t.Cleanup(blobServer.Close)
	f.blobURL = blobServer.URL

	gh := newGitHubClient(apiServer.URL, "test-token")
	gh.sleep = func(time.Duration) { t.Fatal("unexpected rate-limit sleep") }
	return f, &archiver{gh: gh, dir: t.TempDir()}
}

func (f *fakeGitHub) serveRuns(w http.ResponseWriter, r *http.Request) {
	created := r.URL.Query().Get("created")
	f.createdQueries = append(f.createdQueries, created)
	var matching []workflowRun
	for _, run := range f.runs {
		if run.CreatedAt.UTC().Format("2006-01-02") == created {
			matching = append(matching, run)
		}
	}
	writePage(w, r, "workflow_runs", matching)
}

func (f *fakeGitHub) serveJobs(w http.ResponseWriter, r *http.Request) {
	writePage(w, r, "jobs", f.jobs[r.PathValue("run")+"/"+r.PathValue("attempt")])
}

func (f *fakeGitHub) serveLogRedirect(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.ParseInt(r.PathValue("job"), 10, 64)
	if f.expired[id] {
		w.WriteHeader(http.StatusGone)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("%s/blob/%d", f.blobURL, id), http.StatusFound)
}

// count returns how many requests the fake servers saw for a URL path.
func (f *fakeGitHub) count(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[path]
}

// writePage serves one page of items the way GitHub's list endpoints do.
func writePage[T any](w http.ResponseWriter, r *http.Request, key string, items []T) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	size, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
	start := min((page-1)*size, len(items))
	end := min(start+size, len(items))
	json.NewEncoder(w).Encode(map[string]any{"total_count": len(items), key: items[start:end]})
}

// seedTwoAttemptRun adds run 1, created today: attempt 1 (job 11) failed, attempt 2 (job 21) passed.
func seedTwoAttemptRun(f *fakeGitHub) {
	f.runs = []workflowRun{{ID: 1, RunAttempt: 2, HeadSHA: "abc123", Event: "pull_request", CreatedAt: time.Now().UTC()}}
	f.jobs["1/1"] = []job{{ID: 11, Name: "test", Conclusion: "failure"}}
	f.jobs["1/2"] = []job{{ID: 21, Name: "test", Conclusion: "success"}}
	f.logs[11] = "FAIL src/watch.spec.ts > rebuilds on change"
	f.logs[21] = "PASS src/watch.spec.ts"
}

func readGzip(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	text, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	return string(text)
}

func readMeta(t *testing.T, path string) jobMetadata {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var meta jobMetadata
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatal(err)
	}
	return meta
}

func assertExists(t *testing.T, path string, want bool) {
	t.Helper()
	_, err := os.Stat(path)
	if got := err == nil; got != want {
		t.Errorf("%s exists = %v, want %v", path, got, want)
	}
}

// A run on its 2nd attempt gets both attempts' jobs archived: gzipped logs, metadata, attempt.json.
func TestArchiveRepoSavesEveryAttempt(t *testing.T) {
	f, a := newFakeGitHub(t)
	seedTwoAttemptRun(f)

	s, err := a.archiveRepo(context.Background(), "o/r", 0)
	if err != nil {
		t.Fatal(err)
	}
	if s.Runs != 1 || s.Attempts != 2 || s.Saved != 2 || s.Bytes == 0 {
		t.Errorf("summary = %+v, want 1 run, 2 attempts, 2 logs saved, bytes > 0", s)
	}

	first := a.attemptDir("o/r", 1, 1)
	if got := readGzip(t, filepath.Join(first, "11.log.gz")); got != f.logs[11] {
		t.Errorf("attempt 1 log = %q, want %q", got, f.logs[11])
	}
	meta := readMeta(t, filepath.Join(first, "11.json"))
	if meta.Attempt != 1 || meta.LogStatus != "archived" || meta.Run.HeadSHA != "abc123" || meta.Job.Conclusion != "failure" {
		t.Errorf("attempt 1 metadata = %+v", meta)
	}
	second := a.attemptDir("o/r", 1, 2)
	if got := readGzip(t, filepath.Join(second, "21.log.gz")); got != f.logs[21] {
		t.Errorf("attempt 2 log = %q, want %q", got, f.logs[21])
	}
	assertExists(t, filepath.Join(first, "attempt.json"), true)
	assertExists(t, filepath.Join(second, "attempt.json"), true)
}

// A second sync makes no jobs or log requests for attempts that already have attempt.json.
func TestArchiveRepoSkipsCompletedAttempts(t *testing.T) {
	f, a := newFakeGitHub(t)
	seedTwoAttemptRun(f)
	if _, err := a.archiveRepo(context.Background(), "o/r", 0); err != nil {
		t.Fatal(err)
	}

	s, err := a.archiveRepo(context.Background(), "o/r", 0)
	if err != nil {
		t.Fatal(err)
	}
	if s.Attempts != 0 || s.Saved != 0 {
		t.Errorf("second sync summary = %+v, want nothing archived", s)
	}
	for _, path := range []string{
		"/repos/o/r/actions/runs/1/attempts/1/jobs",
		"/repos/o/r/actions/runs/1/attempts/2/jobs",
		"/repos/o/r/actions/jobs/11/logs",
		"/repos/o/r/actions/jobs/21/logs",
	} {
		if n := f.count(path); n != 1 {
			t.Errorf("%s requested %d times over two syncs, want 1", path, n)
		}
	}
}

// After a crash mid-attempt, logs already on disk are not downloaded again and the attempt is completed.
func TestArchiveRepoResumesPartialAttempt(t *testing.T) {
	f, a := newFakeGitHub(t)
	seedTwoAttemptRun(f)
	f.jobs["1/1"] = append(f.jobs["1/1"], job{ID: 12, Name: "lint", Conclusion: "success"})
	f.logs[12] = "lint ok"
	if _, err := a.archiveRepo(context.Background(), "o/r", 0); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after job 11 was saved but before job 12 and the marker.
	first := a.attemptDir("o/r", 1, 1)
	for _, name := range []string{"12.log.gz", "12.json", "attempt.json"} {
		os.Remove(filepath.Join(first, name))
	}

	s, err := a.archiveRepo(context.Background(), "o/r", 0)
	if err != nil {
		t.Fatal(err)
	}
	if s.Attempts != 1 || s.Saved != 1 || s.Skipped != 1 {
		t.Errorf("resume summary = %+v, want 1 attempt, 1 saved, 1 skipped", s)
	}
	if n := f.count("/repos/o/r/actions/jobs/11/logs"); n != 1 {
		t.Errorf("job 11 log requested %d times, want 1", n)
	}
	assertExists(t, filepath.Join(first, "12.log.gz"), true)
	assertExists(t, filepath.Join(first, "attempt.json"), true)
}

// A re-run (attempt 2) of a run synced earlier is picked up on the next sync inside the lookback window.
func TestArchiveRepoPicksUpLaterReRun(t *testing.T) {
	f, a := newFakeGitHub(t)
	seedTwoAttemptRun(f)
	f.runs[0].RunAttempt = 1
	if _, err := a.archiveRepo(context.Background(), "o/r", 0); err != nil {
		t.Fatal(err)
	}
	assertExists(t, filepath.Join(a.attemptDir("o/r", 1, 2), "attempt.json"), false)

	f.mu.Lock()
	f.runs[0].RunAttempt = 2 // a maintainer clicked "re-run failed jobs"
	f.mu.Unlock()
	s, err := a.archiveRepo(context.Background(), "o/r", 0)
	if err != nil {
		t.Fatal(err)
	}
	if s.Attempts != 1 || s.Saved != 1 {
		t.Errorf("re-run sync summary = %+v, want 1 attempt, 1 saved", s)
	}
	assertExists(t, filepath.Join(a.attemptDir("o/r", 1, 2), "21.log.gz"), true)
	if n := f.count("/repos/o/r/actions/jobs/11/logs"); n != 1 {
		t.Errorf("attempt 1 log requested %d times, want 1", n)
	}
}

// A 410 from the logs endpoint writes metadata with log_status "expired", no log file, and counts it.
func TestArchiveRepoRecordsExpiredLog(t *testing.T) {
	f, a := newFakeGitHub(t)
	seedTwoAttemptRun(f)
	f.expired[11] = true

	s, err := a.archiveRepo(context.Background(), "o/r", 0)
	if err != nil {
		t.Fatal(err)
	}
	if s.Expired != 1 || s.Saved != 1 {
		t.Errorf("summary = %+v, want 1 expired, 1 saved", s)
	}
	first := a.attemptDir("o/r", 1, 1)
	if meta := readMeta(t, filepath.Join(first, "11.json")); meta.LogStatus != "expired" {
		t.Errorf("log_status = %q, want expired", meta.LogStatus)
	}
	assertExists(t, filepath.Join(first, "11.log.gz"), false)
	assertExists(t, filepath.Join(first, "11.log.gz.tmp"), false)
	assertExists(t, filepath.Join(first, "attempt.json"), true)
}

// The lookback window is queried one created=YYYY-MM-DD day at a time.
func TestArchiveRepoQueriesEachDay(t *testing.T) {
	f, a := newFakeGitHub(t)
	if _, err := a.archiveRepo(context.Background(), "o/r", 72*time.Hour); err != nil {
		t.Fatal(err)
	}
	today := time.Now().UTC()
	var want []string
	for back := 3; back >= 0; back-- {
		want = append(want, today.AddDate(0, 0, -back).Format("2006-01-02"))
	}
	if !slices.Equal(f.createdQueries, want) {
		t.Errorf("created queries = %v, want %v", f.createdQueries, want)
	}
}

// Runs and jobs listings follow every page until a short page.
func TestListingsFollowPagination(t *testing.T) {
	f, a := newFakeGitHub(t)
	now := time.Now().UTC()
	for i := range 150 {
		f.runs = append(f.runs, workflowRun{ID: int64(i + 1), RunAttempt: 1, CreatedAt: now})
	}
	for i := range 120 {
		f.jobs["1/1"] = append(f.jobs["1/1"], job{ID: int64(1000 + i)})
	}

	runs, err := a.gh.listCompletedRuns(context.Background(), "o/r", now)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 150 {
		t.Errorf("got %d runs, want 150", len(runs))
	}
	jobs, err := a.gh.listAttemptJobs(context.Background(), "o/r", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 120 {
		t.Errorf("got %d jobs, want 120", len(jobs))
	}
}

// A 403 with X-RateLimit-Remaining: 0 sleeps until X-RateLimit-Reset, then retries and succeeds.
func TestGetJSONWaitsOutRateLimit(t *testing.T) {
	f, a := newFakeGitHub(t)
	seedTwoAttemptRun(f)
	f.rateLimitNext = true
	var slept []time.Duration
	a.gh.sleep = func(d time.Duration) { slept = append(slept, d) }

	runs, err := a.gh.listCompletedRuns(context.Background(), "o/r", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Errorf("got %d runs after retry, want 1", len(runs))
	}
	if len(slept) != 1 || slept[0] < 29*time.Second || slept[0] > 32*time.Second {
		t.Errorf("slept %v, want one sleep of about 30s", slept)
	}
	if n := f.count("/repos/o/r/actions/runs"); n != 2 {
		t.Errorf("runs listing requested %d times, want 2", n)
	}
}

// The log redirect to blob storage is followed, and the token is not sent to the blob host.
func TestDownloadJobLogDropsTokenOnRedirect(t *testing.T) {
	f, a := newFakeGitHub(t)
	seedTwoAttemptRun(f)
	f.blobURL = "http://localhost" + f.blobURL[len("http://127.0.0.1"):] // a different host, as in production

	var buf bytes.Buffer
	if err := a.gh.downloadJobLog(context.Background(), "o/r", 11, &buf); err != nil {
		t.Fatal(err)
	}
	if buf.String() != f.logs[11] {
		t.Errorf("log = %q, want %q", buf.String(), f.logs[11])
	}
	if f.apiAuth != "Bearer test-token" {
		t.Errorf("API saw Authorization %q, want the token", f.apiAuth)
	}
	if f.blobAuth != "" {
		t.Errorf("blob host saw Authorization %q, want none", f.blobAuth)
	}
}
