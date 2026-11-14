// Command archiver saves every GitHub Actions job log of the given repos to
// disk before GitHub deletes it (90 days by default). It is the Phase 0
// stopgap from the flaky-test plan; Phase 2 imports what it saves.
//
// Usage: GITHUB_TOKEN=... archiver [-dir archive] [-lookback 168h] owner/repo...
//
// Layout: <dir>/<owner>/<repo>/<run_id>/<attempt>/
//
//	<job_id>.log.gz   the raw job log, gzipped
//	<job_id>.json     jobMetadata for that log
//	attempt.json      written last; its presence means the attempt is fully archived
package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// perPage is the largest page size GitHub's list endpoints accept.
const perPage = 100

// errLogExpired means GitHub no longer has this job's log (404 or 410).
var errLogExpired = errors.New("job log expired")

// githubClient makes authenticated GitHub REST calls and waits out rate limits.
type githubClient struct {
	baseURL string              // "https://api.github.com", or a test server
	token   string              // personal token with public read access
	http    *http.Client        // follows the log redirect; drops the token on the cross-host hop
	sleep   func(time.Duration) // time.Sleep in production; recorded in tests
}

// workflowRun is the subset of a GitHub workflow run the archive keeps.
type workflowRun struct {
	ID         int64     `json:"id"`
	Name       string    `json:"name"`
	RunAttempt int       `json:"run_attempt"` // latest attempt; earlier ones are 1..RunAttempt-1
	HeadSHA    string    `json:"head_sha"`
	HeadBranch string    `json:"head_branch"`
	Event      string    `json:"event"`
	Status     string    `json:"status"`
	Conclusion string    `json:"conclusion"`
	CreatedAt  time.Time `json:"created_at"`
}

// job is the subset of a GitHub job (one job in one run attempt) the archive keeps.
type job struct {
	ID          int64     `json:"id"`
	Name        string    `json:"name"`
	Conclusion  string    `json:"conclusion"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	RunnerName  string    `json:"runner_name"`
	Labels      []string  `json:"labels"`
}

// jobMetadata is written next to each log so Phase 2 can fill workflow_runs and jobs.
type jobMetadata struct {
	Repo       string      `json:"repo"`
	Run        workflowRun `json:"run"`
	Attempt    int         `json:"attempt"`
	Job        job         `json:"job"`
	LogStatus  string      `json:"log_status"` // "archived" or "expired"
	ArchivedAt time.Time   `json:"archived_at"`
}

// logCounts tallies log downloads, for the volume numbers Phase 0 asks for.
type logCounts struct {
	Saved   int
	Skipped int // already on disk from an earlier, interrupted sync
	Expired int
	Bytes   int64 // compressed bytes written
}

// repoSummary is what one sync of one repo did, printed at the end of each run.
type repoSummary struct {
	Repo     string
	Runs     int
	Attempts int // attempts archived this sync (already-complete ones are not counted)
	logCounts
}

// archiver writes run attempts from GitHub into the archive directory.
type archiver struct {
	gh  *githubClient
	dir string
}

// main parses flags, reads GITHUB_TOKEN, archives each repo and prints a summary per repo.
func main() {
	dir := flag.String("dir", "archive", "archive directory")
	lookback := flag.Duration("lookback", 7*24*time.Hour, "re-check runs created within this window (use 2160h for a 90-day backfill)")
	flag.Parse()
	repos := flag.Args()
	if len(repos) == 0 {
		log.Fatal("usage: archiver [-dir archive] [-lookback 168h] owner/repo...")
	}
	for _, repo := range repos {
		if strings.Count(repo, "/") != 1 || strings.Contains(repo, "..") {
			log.Fatalf("%q is not owner/repo", repo)
		}
	}
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		log.Fatal("GITHUB_TOKEN is not set")
	}

	a := &archiver{gh: newGitHubClient("https://api.github.com", token), dir: *dir}
	failed := false
	for _, repo := range repos {
		s, err := a.archiveRepo(context.Background(), repo, *lookback)
		fmt.Printf("%s: %d runs, %d attempts archived, %d logs saved (%.1f MB), %d skipped, %d expired\n",
			s.Repo, s.Runs, s.Attempts, s.Saved, float64(s.Bytes)/1e6, s.Skipped, s.Expired)
		if err != nil {
			log.Printf("%s: %v", repo, err)
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
}

// newGitHubClient returns a client for baseURL that authenticates with token.
// Log downloads redirect to a signed blob URL on another host; net/http drops
// the Authorization header on such cross-host redirects, so the token stays with GitHub.
func newGitHubClient(baseURL, token string) *githubClient {
	return &githubClient{
		baseURL: baseURL,
		token:   token,
		sleep:   time.Sleep,
		http:    &http.Client{Timeout: 10 * time.Minute}, // large logs take a while
	}
}

// add folds another tally into this one.
func (c *logCounts) add(other logCounts) {
	c.Saved += other.Saved
	c.Skipped += other.Skipped
	c.Expired += other.Expired
	c.Bytes += other.Bytes
}

// do sends an authenticated GET, sleeping and retrying (up to 3 tries) when rate-limited.
func (c *githubClient) do(ctx context.Context, path string) (*http.Response, error) {
	for try := 1; ; try++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+c.token)
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
		resp, err := c.http.Do(req)
		if err != nil {
			return nil, err
		}
		wait, limited := rateLimitWait(resp)
		if !limited {
			return resp, nil
		}
		resp.Body.Close()
		if try == 3 {
			return nil, fmt.Errorf("GET %s: still rate-limited after %d tries", path, try)
		}
		c.sleep(wait)
	}
}

// rateLimitWait reports whether resp is a rate-limit rejection and how long to wait before retrying.
func rateLimitWait(resp *http.Response) (time.Duration, bool) {
	if resp.StatusCode != http.StatusForbidden && resp.StatusCode != http.StatusTooManyRequests {
		return 0, false
	}
	if secs, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
		return time.Duration(secs) * time.Second, true // secondary rate limit
	}
	if resp.Header.Get("X-RateLimit-Remaining") != "0" {
		return 0, false // a real 403, not a rate limit
	}
	reset, err := strconv.ParseInt(resp.Header.Get("X-RateLimit-Reset"), 10, 64)
	if err != nil {
		return time.Minute, true
	}
	return max(time.Until(time.Unix(reset, 0))+time.Second, time.Second), true
}

// getJSON GETs an API path and decodes the response into out, sleeping until reset when rate-limited.
func (c *githubClient) getJSON(ctx context.Context, path string, out any) error {
	resp, err := c.do(ctx, path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("GET %s: %s: %s", path, resp.Status, body)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// listCompletedRuns returns every completed run of repo created on one UTC day, across all pages.
// It queries one day at a time because GitHub caps a filtered run listing at 1,000 results.
func (c *githubClient) listCompletedRuns(ctx context.Context, repo string, day time.Time) ([]workflowRun, error) {
	date := day.UTC().Format("2006-01-02")
	var runs []workflowRun
	for page := 1; ; page++ {
		var body struct {
			TotalCount   int           `json:"total_count"`
			WorkflowRuns []workflowRun `json:"workflow_runs"`
		}
		path := fmt.Sprintf("/repos/%s/actions/runs?status=completed&created=%s&per_page=%d&page=%d", repo, date, perPage, page)
		if err := c.getJSON(ctx, path, &body); err != nil {
			return nil, err
		}
		if page == 1 && body.TotalCount > 1000 {
			log.Printf("%s: %d runs on %s, over GitHub's 1,000-result cap; some will be missed", repo, body.TotalCount, date)
		}
		runs = append(runs, body.WorkflowRuns...)
		if len(body.WorkflowRuns) < perPage {
			return runs, nil
		}
	}
}

// listAttemptJobs returns every job of one attempt of one run, across all pages.
func (c *githubClient) listAttemptJobs(ctx context.Context, repo string, runID int64, attempt int) ([]job, error) {
	var jobs []job
	for page := 1; ; page++ {
		var body struct {
			Jobs []job `json:"jobs"`
		}
		path := fmt.Sprintf("/repos/%s/actions/runs/%d/attempts/%d/jobs?per_page=%d&page=%d", repo, runID, attempt, perPage, page)
		if err := c.getJSON(ctx, path, &body); err != nil {
			return nil, err
		}
		jobs = append(jobs, body.Jobs...)
		if len(body.Jobs) < perPage {
			return jobs, nil
		}
	}
}

// downloadJobLog streams a job's log into w and returns errLogExpired if GitHub no longer has it.
func (c *githubClient) downloadJobLog(ctx context.Context, repo string, jobID int64, w io.Writer) error {
	path := fmt.Sprintf("/repos/%s/actions/jobs/%d/logs", repo, jobID)
	resp, err := c.do(ctx, path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		_, err := io.Copy(w, resp.Body)
		return err
	case http.StatusNotFound, http.StatusGone:
		return errLogExpired
	}
	return fmt.Errorf("GET %s: %s", path, resp.Status)
}

// archiveRepo archives every not-yet-complete attempt of runs created in the last lookback window.
// Re-listing a window (not a cursor) is what catches re-runs, which reuse an old run's created_at.
// A failed attempt is reported and skipped so the rest still get archived; the next sync retries it.
func (a *archiver) archiveRepo(ctx context.Context, repo string, lookback time.Duration) (repoSummary, error) {
	summary := repoSummary{Repo: repo}
	today := time.Now().UTC().Truncate(24 * time.Hour)
	var runs []workflowRun
	for day := today.Add(-lookback).Truncate(24 * time.Hour); !day.After(today); day = day.AddDate(0, 0, 1) {
		dayRuns, err := a.gh.listCompletedRuns(ctx, repo, day)
		if err != nil {
			return summary, err
		}
		runs = append(runs, dayRuns...)
	}
	summary.Runs = len(runs)

	var failures []error
	for _, run := range runs {
		for attempt := 1; attempt <= run.RunAttempt; attempt++ {
			marker := filepath.Join(a.attemptDir(repo, run.ID, attempt), "attempt.json")
			if _, err := os.Stat(marker); err == nil {
				continue
			}
			counts, err := a.archiveAttempt(ctx, repo, run, attempt)
			summary.add(counts)
			if err != nil {
				failures = append(failures, fmt.Errorf("run %d attempt %d: %w", run.ID, attempt, err))
				continue
			}
			summary.Attempts++
		}
	}
	return summary, errors.Join(failures...)
}

// archiveAttempt saves every job log of one run attempt, then writes attempt.json to mark it complete.
func (a *archiver) archiveAttempt(ctx context.Context, repo string, run workflowRun, attempt int) (logCounts, error) {
	jobs, err := a.gh.listAttemptJobs(ctx, repo, run.ID, attempt)
	if err != nil {
		return logCounts{}, err
	}
	var counts logCounts
	for _, j := range jobs {
		c, err := a.saveJobLog(ctx, jobMetadata{Repo: repo, Run: run, Attempt: attempt, Job: j})
		counts.add(c)
		if err != nil {
			return counts, fmt.Errorf("job %d: %w", j.ID, err)
		}
	}
	record := struct {
		Repo    string      `json:"repo"`
		Run     workflowRun `json:"run"`
		Attempt int         `json:"attempt"`
		Jobs    int         `json:"jobs"`
	}{repo, run, attempt, len(jobs)}
	marker := filepath.Join(a.attemptDir(repo, run.ID, attempt), "attempt.json")
	return counts, writeFileAtomic(marker, func(w io.Writer) error { return json.NewEncoder(w).Encode(record) })
}

// saveJobLog downloads one job's log as gzip and writes its metadata; it skips logs already on disk.
// The metadata file is written after the log, so its presence means both are complete.
func (a *archiver) saveJobLog(ctx context.Context, meta jobMetadata) (logCounts, error) {
	dir := a.attemptDir(meta.Repo, meta.Run.ID, meta.Attempt)
	logPath := filepath.Join(dir, fmt.Sprintf("%d.log.gz", meta.Job.ID))
	metaPath := filepath.Join(dir, fmt.Sprintf("%d.json", meta.Job.ID))
	if _, err := os.Stat(metaPath); err == nil {
		return logCounts{Skipped: 1}, nil
	}

	err := writeFileAtomic(logPath, func(w io.Writer) error {
		gz := gzip.NewWriter(w)
		if err := a.gh.downloadJobLog(ctx, meta.Repo, meta.Job.ID, gz); err != nil {
			return err
		}
		return gz.Close()
	})
	var counts logCounts
	switch {
	case errors.Is(err, errLogExpired):
		meta.LogStatus = "expired"
		counts.Expired = 1
	case err != nil:
		return logCounts{}, err
	default:
		info, err := os.Stat(logPath)
		if err != nil {
			return logCounts{}, err
		}
		meta.LogStatus = "archived"
		counts.Saved = 1
		counts.Bytes = info.Size()
	}
	meta.ArchivedAt = time.Now().UTC()
	return counts, writeFileAtomic(metaPath, func(w io.Writer) error { return json.NewEncoder(w).Encode(meta) })
}

// attemptDir returns <dir>/<owner>/<repo>/<run_id>/<attempt>.
func (a *archiver) attemptDir(repo string, runID int64, attempt int) string {
	return filepath.Join(a.dir, filepath.FromSlash(repo), strconv.FormatInt(runID, 10), strconv.Itoa(attempt))
}

// writeFileAtomic writes via a temp file and rename, so a crash never leaves a partial file behind.
func writeFileAtomic(path string, write func(io.Writer) error) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := write(f); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}
