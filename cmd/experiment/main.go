// Command experiment is the Section 4 measurement client for ImageLab
// Version 2. It drives the observation API exactly like frontend/app.js:
//
//   - short mode: one prompt GET immediately, then one every second;
//   - long mode: one prompt GET to establish a baseline, then after/wait
//     requests, each followed immediately by the next.
//
// The browser's Date.now() is replaced by Go's time.Now() (both wall clock);
// every request and receipt is timestamped so completion-detection delay can
// be compared across conditions and modes. Detection delay is the client
// receipt time of the terminal snapshot minus the server's post-commit
// wall-clock log time for the same job, joined by job id and version.
//
// It appends one row per run to a CSV and writes a JSONL raw log per run.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// snapshot mirrors the public job representation returned by the API.
type snapshot struct {
	Status            string          `json:"status"`
	Version           int             `json:"version"`
	Stage             *string         `json:"stage"`
	VariantsCompleted int             `json:"variants_completed"`
	SimulationStep    *int            `json:"simulation_step"`
	QueuedAt          string          `json:"queued_at"`
	StartedAt         *string         `json:"started_at"`
	CompletedAt       *string         `json:"completed_at"`
	FailedAt          *string         `json:"failed_at"`
	Error             *string         `json:"error"`
	Variants          json.RawMessage `json:"variants"`
}

// accept mirrors the 202 upload response.
type accept struct {
	JobID     string `json:"job_id"`
	ImageID   string `json:"image_id"`
	Status    string `json:"status"`
	StatusURL string `json:"status_url"`
	Version   int    `json:"version"`
}

// event is one line of the per-run JSONL raw log.
type event struct {
	At      string `json:"at"`
	Run     string `json:"run"`
	Event   string `json:"event"`
	Seq     int    `json:"seq,omitempty"`
	Mode    string `json:"mode,omitempty"`
	HTTP    int    `json:"http,omitempty"`
	Version int    `json:"version,omitempty"`
	Status  string `json:"status,omitempty"`
	Note    string `json:"note,omitempty"`
}

type config struct {
	base             string
	image            string
	condition        string
	label            string
	mode             string
	reps             int
	latencyMS        int
	quietDelayMS     int
	simulateDurMS    int
	simulateIntMS    int
	serverLog        string
	csvPath          string
	rawDir           string
	shortIntervalMS  int
	longWaitSeconds  int
	requestTimeoutMS int
}

type result struct {
	runID          string
	jobID          string
	image          string
	condition      string
	label          string
	mode           string
	rep            int
	latencySetting string
	quietDelayMS   int
	simulateDurMS  int
	simulateIntMS  int
	latencyMS      int
	uploadAckMS    float64
	promptGetMS    float64
	detectionMS    float64
	processingMS   float64
	jobDurationMS  float64
	initial200     int
	newer200       int
	unchanged200   int
	noContent204   int
	errors         int
	cancellations  int
	totalRequests  int
	// dbReads counts observation requests that reached the server and read
	// authoritative state (every 200 and every deadline recheck that became a
	// 204). The held wait itself never polls the database.
	dbReads            int
	baselineVersion    int
	terminalVersion    int
	distinctNewSeen    int
	versionCoverage    float64
	variantsCompleted  int
	status             string
	serverCompletedAt  string
	clientCompletedAt  string
	observationStartAt string
	notes              string

	// SSE-specific counters (MEAS-05). A stream GET is one HTTP request that
	// stays open; job events are the named frames inside it; heartbeats are
	// SSE comment frames and are not job updates.
	sseStreamGets     int
	sseJobEvents      int
	sseHeartbeats     int
	firstEventMS      float64
	streamSeconds     float64
	peakActiveStreams int
	sseDBReads        int
	versionsReceived  int
	versionGaps       int
}

func main() {
	var cfg config
	flag.StringVar(&cfg.base, "base", "http://localhost:4000", "API base URL")
	flag.StringVar(&cfg.image, "image", "", "Path to the test image to upload")
	flag.StringVar(&cfg.condition, "condition", "B", "Condition id (A, B, C1, C2, C3)")
	flag.StringVar(&cfg.label, "label", "", "Human label for the condition")
	flag.StringVar(&cfg.mode, "mode", "short", "Observation mode: short, long, or sse")
	flag.IntVar(&cfg.reps, "reps", 3, "Repetitions for this condition/mode")
	flag.IntVar(&cfg.latencyMS, "latency", 0, "Additional round-trip latency in ms (split before/after each observation request)")
	flag.IntVar(&cfg.quietDelayMS, "quiet-delay", 0, "Recorded quiet delay (ms), for the CSV")
	flag.IntVar(&cfg.simulateDurMS, "simulate-duration", 0, "Recorded simulated phase duration (ms), for the CSV")
	flag.IntVar(&cfg.simulateIntMS, "simulate-interval", 0, "Recorded simulated update interval (ms), for the CSV")
	flag.StringVar(&cfg.serverLog, "server-log", "", "Server log file to parse terminal commit times from")
	flag.StringVar(&cfg.csvPath, "out", "experiments/results/runs.csv", "CSV file to append rows to")
	flag.StringVar(&cfg.rawDir, "rawdir", "experiments/results/raw", "Directory for per-run JSONL raw logs")
	flag.IntVar(&cfg.shortIntervalMS, "short-interval", 1000, "Short polling interval (ms)")
	flag.IntVar(&cfg.longWaitSeconds, "long-wait", 20, "Long polling wait parameter (seconds)")
	flag.IntVar(&cfg.requestTimeoutMS, "request-timeout", 30000, "Per-request client timeout (ms)")
	flag.Parse()

	if cfg.image == "" {
		fmt.Fprintln(os.Stderr, "error: -image is required")
		os.Exit(2)
	}
	if cfg.mode != "short" && cfg.mode != "long" && cfg.mode != "sse" {
		fmt.Fprintln(os.Stderr, "error: -mode must be short, long, or sse")
		os.Exit(2)
	}
	if err := os.MkdirAll(cfg.rawDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "error: cannot create raw dir:", err)
		os.Exit(1)
	}

	client := &http.Client{Timeout: time.Duration(cfg.requestTimeoutMS) * time.Millisecond}

	for rep := 1; rep <= cfg.reps; rep++ {
		var res *result
		var err error
		if cfg.mode == "sse" {
			res, err = runOnceSSE(client, cfg, rep)
		} else {
			res, err = runOnce(client, cfg, rep)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "run %s/%s rep %d failed: %v\n", cfg.condition, cfg.mode, rep, err)
			continue
		}
		if err := appendCSV(cfg.csvPath, res); err != nil {
			fmt.Fprintln(os.Stderr, "error writing CSV:", err)
			os.Exit(1)
		}
		fmt.Printf("run %-12s mode=%-5s rep=%d detection=%7.1fms gets=%d events=%d coverage=%.2f\n",
			res.runID, res.mode, rep, res.detectionMS, observationGets(res), res.sseJobEvents, res.versionCoverage)
	}
}

// runOnce uploads one image and observes it to a terminal state, returning the
// metrics for one run and writing the per-run raw JSONL log.
func runOnce(client *http.Client, cfg config, rep int) (*result, error) {
	runID := fmt.Sprintf("%s-%s-%d-%s", cfg.condition, cfg.mode, rep, time.Now().Format("150405.000"))

	rawPath := filepath.Join(cfg.rawDir, runID+".jsonl")
	raw, err := os.Create(rawPath)
	if err != nil {
		return nil, err
	}
	defer raw.Close()

	logEvent := func(ev event) {
		ev.Run = runID
		ev.At = time.Now().UTC().Format(time.RFC3339Nano)
		b, _ := json.Marshal(ev)
		raw.Write(b)
		raw.Write([]byte("\n"))
	}

	// --- upload ---
	uploadStart := time.Now()
	accepted, err := upload(client, cfg.base, cfg.image)
	ackMS := float64(time.Since(uploadStart).Microseconds()) / 1000.0
	if err != nil {
		logEvent(event{Event: "upload_error", Note: err.Error()})
		return nil, err
	}
	logEvent(event{Event: "ack", HTTP: 202, Version: accepted.Version, Note: accepted.JobID})

	// latencySetting documents the actual added-latency configuration
	// verbatim (the brief asks to report the actual setting if tooling cannot
	// produce exactly 200 ms).
	latencySetting := "none"
	if cfg.latencyMS > 0 {
		latencySetting = fmt.Sprintf("%dms added round-trip (%dms before + %dms after each observation request)",
			cfg.latencyMS, cfg.latencyMS/2, cfg.latencyMS/2)
	}

	res := &result{
		runID:              runID,
		jobID:              accepted.JobID,
		image:              filepath.Base(cfg.image),
		condition:          cfg.condition,
		label:              cfg.label,
		mode:               cfg.mode,
		rep:                rep,
		latencySetting:     latencySetting,
		quietDelayMS:       cfg.quietDelayMS,
		simulateDurMS:      cfg.simulateDurMS,
		simulateIntMS:      cfg.simulateIntMS,
		latencyMS:          cfg.latencyMS,
		uploadAckMS:        ackMS,
		status:             accepted.Status,
		observationStartAt: time.Now().UTC().Format(time.RFC3339Nano),
		notes:              runNotes(cfg),
	}

	statusURL := cfg.base + accepted.StatusURL
	latencyHalf := time.Duration(cfg.latencyMS/2) * time.Millisecond

	// seen records every version number actually received from an observation
	// GET. The baseline is the first observed version, so versions that only
	// existed in the 202 response are not counted.
	seen := map[int]bool{}
	versionFirstSeen := -1
	versionLastSeen := accepted.Version
	var terminal snapshot

	// observe performs one GET and classifies the response.
	observe := func(seq int, url string) (snap *snapshot, httpStatus int, isTerminal bool, err error) {
		if latencyHalf > 0 {
			time.Sleep(latencyHalf)
		}
		logEvent(event{Event: "request", Seq: seq, Mode: cfg.mode, Note: url})
		rtStart := time.Now()
		resp, err := client.Get(url)
		if err != nil {
			logEvent(event{Event: "error", Seq: seq, Note: err.Error()})
			return nil, 0, false, err
		}
		defer resp.Body.Close()
		res.totalRequests++

		if resp.StatusCode == http.StatusNoContent {
			res.noContent204++
			res.dbReads++
			if latencyHalf > 0 {
				time.Sleep(latencyHalf)
			}
			logEvent(event{Event: "response", Seq: seq, HTTP: 204, Mode: cfg.mode})
			return nil, 204, false, nil
		}
		if resp.StatusCode != http.StatusOK {
			res.errors++
			if latencyHalf > 0 {
				time.Sleep(latencyHalf)
			}
			logEvent(event{Event: "response", Seq: seq, HTTP: resp.StatusCode, Mode: cfg.mode})
			return nil, resp.StatusCode, false, fmt.Errorf("HTTP %d", resp.StatusCode)
		}

		var s snapshot
		if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
			res.errors++
			logEvent(event{Event: "error", Seq: seq, Note: err.Error()})
			return nil, 200, false, err
		}
		res.dbReads++
		if seq == 1 {
			// Prompt-GET timing check: the real HTTP round trip, excluding the
			// documented injected latency sleeps.
			res.promptGetMS = float64(time.Since(rtStart).Microseconds()) / 1000.0
		}
		if latencyHalf > 0 {
			time.Sleep(latencyHalf)
		}

		if versionFirstSeen == -1 {
			versionFirstSeen = s.Version
		}
		if !seen[s.Version] {
			seen[s.Version] = true
		}
		logEvent(event{Event: "response", Seq: seq, HTTP: 200, Version: s.Version, Status: s.Status, Mode: cfg.mode})

		term := s.Status == "completed" || s.Status == "failed"
		return &s, 200, term, nil
	}

	// Initial prompt snapshot (API-01), just like the frontend.
	seq := 1
	snap, st, term, err := observe(seq, statusURL)
	if err != nil {
		// A transport error on the very first read ends the run.
		if st == 0 {
			return nil, err
		}
	}
	if st == 200 {
		res.initial200++
		if snap != nil {
			versionLastSeen = snap.Version
			applySnapshot(res, snap)
			terminal = *snap
		}
	}

	if term {
		finalize(res, cfg, terminal, seen, versionFirstSeen, logEvent)
		return res, nil
	}

	// --- observation loop ---
	for {
		seq++
		var url string
		if cfg.mode == "long" {
			url = fmt.Sprintf("%s?after=%d&wait=%d", statusURL, versionLastSeen, cfg.longWaitSeconds)
		} else {
			time.Sleep(time.Duration(cfg.shortIntervalMS) * time.Millisecond)
			url = statusURL
		}

		s, status, term, err := observe(seq, url)
		if err != nil {
			if status == 0 {
				// Transport failure or timeout: stop, but keep what we have.
				res.errors++
				break
			}
			continue
		}
		if status == 204 {
			continue
		}
		if s == nil {
			continue
		}

		// Count 200s as newer or unchanged relative to the previously seen
		// version (the initial 200 is counted separately).
		if seq > 1 {
			if s.Version > versionLastSeen {
				res.newer200++
			} else {
				res.unchanged200++
			}
		}
		versionLastSeen = s.Version
		applySnapshot(res, s)
		terminal = *s
		if term {
			break
		}
	}

	finalize(res, cfg, terminal, seen, versionFirstSeen, logEvent)
	return res, nil
}

// observationGets reports the observation HTTP requests started by a run.
// For SSE that is the stream GET, not the events carried inside it (MEAS-10).
func observationGets(res *result) int {
	if res.mode == "sse" {
		return res.sseStreamGets
	}
	return res.totalRequests
}

// runOnceSSE performs one measured SSE observation. It opens a single stream
// GET, reads the named event frames until the terminal snapshot, and records
// stream activity separately from job updates and heartbeats.
func runOnceSSE(client *http.Client, cfg config, rep int) (*result, error) {
	runID := fmt.Sprintf("%s-%s-%d-%s", cfg.condition, cfg.mode, rep, time.Now().Format("150405.000"))

	rawPath := filepath.Join(cfg.rawDir, runID+".jsonl")
	raw, err := os.Create(rawPath)
	if err != nil {
		return nil, err
	}
	defer raw.Close()

	logEvent := func(ev event) {
		ev.Run = runID
		ev.At = time.Now().UTC().Format(time.RFC3339Nano)
		b, _ := json.Marshal(ev)
		raw.Write(b)
		raw.Write([]byte("\n"))
	}

	// --- upload (the only POST; it is never counted as observation) ---
	uploadStart := time.Now()
	accepted, err := upload(client, cfg.base, cfg.image)
	ackMS := float64(time.Since(uploadStart).Microseconds()) / 1000.0
	if err != nil {
		logEvent(event{Event: "upload_error", Note: err.Error()})
		return nil, err
	}
	logEvent(event{Event: "ack", HTTP: 202, Version: accepted.Version, Note: accepted.JobID})

	res := &result{
		runID:              runID,
		jobID:              accepted.JobID,
		image:              filepath.Base(cfg.image),
		condition:          cfg.condition,
		label:              cfg.label,
		mode:               cfg.mode,
		rep:                rep,
		latencySetting:     "none",
		quietDelayMS:       cfg.quietDelayMS,
		simulateDurMS:      cfg.simulateDurMS,
		simulateIntMS:      cfg.simulateIntMS,
		uploadAckMS:        ackMS,
		status:             accepted.Status,
		observationStartAt: time.Now().UTC().Format(time.RFC3339Nano),
		notes:              runNotes(cfg),
	}

	statusURL := cfg.base + accepted.StatusURL
	eventsURL := statusURL + "/events"

	// A stream has no total timeout: it is meant to stay open for the whole
	// job. Cancellation is explicit once the terminal frame arrives.
	streamClient := &http.Client{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, eventsURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	streamStart := time.Now()
	logEvent(event{Event: "sse_connect", Seq: 1, Mode: cfg.mode, Note: eventsURL})
	resp, err := streamClient.Do(req)
	if err != nil {
		logEvent(event{Event: "error", Seq: 1, Note: err.Error()})
		res.errors++
		return nil, err
	}
	defer resp.Body.Close()

	res.sseStreamGets++
	res.totalRequests = 1
	res.peakActiveStreams = 1
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		logEvent(event{Event: "response", HTTP: resp.StatusCode, Mode: cfg.mode})
		res.errors++
		return nil, fmt.Errorf("SSE stream returned HTTP %d: %s", resp.StatusCode, string(body))
	}
	contentType := resp.Header.Get("Content-Type")
	logEvent(event{Event: "sse_open", HTTP: resp.StatusCode, Mode: cfg.mode, Note: contentType})
	if !strings.HasPrefix(contentType, "text/event-stream") {
		res.errors++
		return nil, fmt.Errorf("SSE stream had Content-Type %q", contentType)
	}

	seen := map[int]bool{}
	versionFirstSeen := -1
	lastVersion := -1
	var terminal snapshot
	terminalSeen := false

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var eventName string
	var dataLines []string

	dispatch := func() bool {
		if len(dataLines) == 0 {
			eventName = ""
			return false
		}
		payload := strings.Join(dataLines, "\n")
		name := eventName
		eventName = ""
		dataLines = nil

		if name != "job-update" {
			logEvent(event{Event: "sse_unknown_event", Mode: cfg.mode, Note: name})
			return false
		}

		var s snapshot
		if err := json.Unmarshal([]byte(payload), &s); err != nil {
			res.errors++
			logEvent(event{Event: "sse_parse_error", Mode: cfg.mode, Note: err.Error()})
			return false
		}

		res.sseJobEvents++
		if res.firstEventMS == 0 {
			res.firstEventMS = float64(time.Since(streamStart).Microseconds()) / 1000.0
			res.promptGetMS = res.firstEventMS
		}
		if versionFirstSeen == -1 {
			versionFirstSeen = s.Version
		}
		seen[s.Version] = true
		lastVersion = s.Version
		logEvent(event{Event: "sse_job_update", Version: s.Version, Status: s.Status, Mode: cfg.mode})

		applySnapshot(res, &s)
		terminal = s
		if s.Status == "completed" || s.Status == "failed" {
			res.clientCompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
			return true
		}
		return false
	}

	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			if dispatch() {
				terminalSeen = true
			}
		case strings.HasPrefix(line, ":"):
			res.sseHeartbeats++
			logEvent(event{Event: "sse_heartbeat", Mode: cfg.mode})
		case strings.HasPrefix(line, "event:"):
			eventName = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			value := strings.TrimPrefix(line, "data:")
			value = strings.TrimPrefix(value, " ")
			dataLines = append(dataLines, value)
		}
		if terminalSeen {
			break
		}
	}
	res.streamSeconds = time.Since(streamStart).Seconds()

	if !terminalSeen {
		if err := scanner.Err(); err != nil && !errors.Is(err, context.Canceled) {
			res.errors++
			logEvent(event{Event: "sse_scan_error", Mode: cfg.mode, Note: err.Error()})
		}
		logEvent(event{Event: "sse_closed", Mode: cfg.mode, Version: lastVersion})
	}

	// The terminal event was received; stop reading and close the stream.
	cancel()

	finalize(res, cfg, terminal, seen, versionFirstSeen, logEvent)

	// The server logs its own stream summary when the handler exits. Read it
	// so the run records authoritative observation database reads and peak
	// active handlers rather than inferring them from the client.
	if cfg.serverLog != "" {
		for attempt := 0; attempt < 20; attempt++ {
			if stats, ok := readSSEStreamStats(cfg.serverLog, res.jobID); ok {
				res.sseDBReads = stats.dbReads
				res.dbReads = stats.dbReads
				if stats.peak > res.peakActiveStreams {
					res.peakActiveStreams = stats.peak
				}
				break
			}
			time.Sleep(25 * time.Millisecond)
		}
	}

	return res, nil
}

// sseStreamStats is the server's own account of one stream: how many database
// reads it performed and the peak number of concurrently active handlers.
type sseStreamStats struct {
	dbReads int
	peak    int
}

var (
	sseDBReadsRe = regexp.MustCompile(`db_reads=(\d+)`)
	ssePeakRe    = regexp.MustCompile(`peak_active_streams=(\d+)`)
)

// sseStreamStats scans the server log for the last "sse stream ended" line for
// this job. Joining by job id keeps interleaved runs apart.
func readSSEStreamStats(logPath, jobID string) (sseStreamStats, bool) {
	data, err := os.ReadFile(logPath)
	if err != nil {
		return sseStreamStats{}, false
	}
	var found sseStreamStats
	ok := false
	for _, line := range bytes.Split(data, []byte("\n")) {
		if !bytes.Contains(line, []byte("sse stream ended")) || !bytes.Contains(line, []byte(jobID)) {
			continue
		}
		db := sseDBReadsRe.FindSubmatch(line)
		peak := ssePeakRe.FindSubmatch(line)
		if db == nil || peak == nil {
			continue
		}
		found.dbReads, _ = strconv.Atoi(string(db[1]))
		found.peak, _ = strconv.Atoi(string(peak[1]))
		ok = true
	}
	return found, ok
}

// applySnapshot copies progress fields from a snapshot onto the result.
func applySnapshot(res *result, s *snapshot) {
	res.status = s.Status
	res.variantsCompleted = s.VariantsCompleted
}

// finalize fills the derived metrics, joins the server commit time, and logs
// the terminal event.
func finalize(res *result, cfg config, terminal snapshot, seen map[int]bool, versionFirstSeen int, logEvent func(event)) {
	if versionFirstSeen == -1 {
		versionFirstSeen = terminal.Version
	}
	res.baselineVersion = versionFirstSeen
	res.terminalVersion = terminal.Version

	// Distinct versions received, and the "new" versions beyond the baseline.
	distinct := 0
	for v := 1; v <= terminal.Version; v++ {
		if seen[v] {
			distinct++
		}
	}
	res.versionsReceived = distinct
	if distinct > 0 {
		res.distinctNewSeen = distinct - 1
	}
	// Version coverage compares the new versions received with the versions
	// committed after the baseline through the terminal version, so gaps lower
	// the ratio (Section 6). A gap is expected for SSE when notifications are
	// coalesced and is not by itself data loss (MEAS-05).
	if span := terminal.Version - res.baselineVersion; span > 0 {
		res.versionCoverage = float64(res.distinctNewSeen) / float64(span)
		res.versionGaps = span - res.distinctNewSeen
		if res.versionGaps < 0 {
			res.versionGaps = 0
		}
	} else {
		res.versionCoverage = 1
	}

	// SSE records the terminal receipt time in the event loop, before any
	// post-processing, because that timestamp is the detection measurement.
	// Polling records it here, after the loop ends. Either way the join to the
	// server's post-commit log time happens below.
	if res.clientCompletedAt == "" {
		res.clientCompletedAt = logEventReturningNow(logEvent, res, terminal)
	} else {
		logEvent(event{Event: "terminal", HTTP: 200, Version: terminal.Version, Status: terminal.Status})
	}

	// processing duration and job duration come from server timestamps.
	if terminal.StartedAt != nil {
		if start, err := time.Parse(time.RFC3339Nano, *terminal.StartedAt); err == nil {
			if terminal.CompletedAt != nil {
				if end, err := time.Parse(time.RFC3339Nano, *terminal.CompletedAt); err == nil {
					res.processingMS = float64(end.Sub(start).Microseconds()) / 1000.0
				}
			}
		}
	}
	if terminal.CompletedAt != nil {
		if end, err := time.Parse(time.RFC3339Nano, *terminal.CompletedAt); err == nil {
			if q, err := time.Parse(time.RFC3339Nano, terminal.QueuedAt); err == nil {
				res.jobDurationMS = float64(end.Sub(q).Microseconds()) / 1000.0
			}
		}
		res.serverCompletedAt = *terminal.CompletedAt
	}

	// Detection delay: client receipt minus the server's post-commit log time.
	if cfg.serverLog != "" {
		if commit, ok := terminalCommitTime(cfg.serverLog, res.jobID, terminal.Status); ok {
			receipt, _ := time.Parse(time.RFC3339Nano, res.clientCompletedAt)
			res.detectionMS = float64(receipt.Sub(commit).Microseconds()) / 1000.0
		}
	}
}

// logEventReturningNow logs a terminal event and returns the receipt time.
func logEventReturningNow(logEvent func(event), res *result, terminal snapshot) string {
	now := time.Now().UTC()
	logEvent(event{Event: "terminal", HTTP: 200, Version: terminal.Version, Status: terminal.Status})
	return now.Format(time.RFC3339Nano)
}

// upload posts the image and decodes the 202 acceptance body.
func upload(client *http.Client, base, imagePath string) (*accept, error) {
	f, err := os.Open(imagePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	part, err := w.CreateFormFile("file", filepath.Base(imagePath))
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(part, f); err != nil {
		return nil, err
	}
	w.Close()

	req, err := http.NewRequest(http.MethodPost, base+"/v1/images", &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("upload returned HTTP %d: %s", resp.StatusCode, string(b))
	}
	var a accept
	if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
		return nil, err
	}
	return &a, nil
}

var commitRe = regexp.MustCompile(`status=(\w+).*committed_at=(\S+)`)

// terminalCommitTime scans the server log for the post-commit wall-clock time
// of a specific job's terminal commit. Joining by job id keeps runs that share
// one server process (both modes, every repetition) apart. The log line is:
//
//	msg="job change committed" job_id=... status=completed version=10 committed_at=2026-...Z
func terminalCommitTime(logPath, jobID, terminalStatus string) (time.Time, bool) {
	data, err := os.ReadFile(logPath)
	if err != nil {
		return time.Time{}, false
	}
	var latest time.Time
	found := false
	for _, line := range bytes.Split(data, []byte("\n")) {
		if !bytes.Contains(line, []byte("job change committed")) {
			continue
		}
		if !bytes.Contains(line, []byte(jobID)) {
			continue
		}
		m := commitRe.FindSubmatch(line)
		if m == nil || string(m[1]) != terminalStatus {
			continue
		}
		if ts, err := time.Parse(time.RFC3339Nano, string(m[2])); err == nil {
			if !found || ts.After(latest) {
				latest = ts
				found = true
			}
		}
	}
	return latest, found
}

// runNotes records the timing semantics and applied workload controls for a
// run, so the methods section can state exactly how each mode was driven.
func runNotes(cfg config) string {
	var parts []string
	switch cfg.mode {
	case "short":
		parts = append(parts, fmt.Sprintf("short: %dms delay after each response, no overlapping requests", cfg.shortIntervalMS))
	case "sse":
		parts = append(parts, "sse: one stream GET; server sends the initial snapshot then one job-update per committed version; no polling and no id replay")
	default:
		parts = append(parts, fmt.Sprintf("long: next request immediately after each active 200 or 204, wait=%ds", cfg.longWaitSeconds))
	}
	if cfg.quietDelayMS > 0 {
		parts = append(parts, fmt.Sprintf("server quiet-delay=%dms (no intermediate updates)", cfg.quietDelayMS))
	}
	if cfg.simulateDurMS > 0 {
		parts = append(parts, fmt.Sprintf("server simulated phase=%dms at %dms updates (labelled simulated, not real progress)", cfg.simulateDurMS, cfg.simulateIntMS))
	}
	if cfg.latencyMS > 0 {
		parts = append(parts, fmt.Sprintf("client added %dms round-trip", cfg.latencyMS))
	}
	parts = append(parts, "detection = client receipt - server post-commit log time, joined by job id")
	return strings.Join(parts, "; ")
}

// appendCSV appends one result row. The first columns are the exact set the
// brief requires; the trailing columns add supporting detail.
func appendCSV(path string, res *result) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	needHeader := true
	if fi, err := os.Stat(path); err == nil && fi.Size() > 0 {
		needHeader = false
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// The first block follows the column list recommended by the Version 3
	// brief; the second keeps the Version 2 supporting detail so existing
	// analysis can still read a run.
	header := []string{
		"run_id", "condition", "condition_label", "mode", "repetition", "job_id", "image",
		"baseline_version", "terminal_version", "observation_gets", "sse_stream_gets",
		"job_events_received", "heartbeats", "versions_received", "version_gaps",
		"processing_ms", "detection_ms", "observation_db_reads", "stream_seconds",
		"peak_active_streams", "notes",
		"requests_started", "initial_200", "newer_200", "unchanged_200", "no_change_204",
		"retrieval_errors", "cancellations", "distinct_new_versions_seen", "version_coverage",
		"job_duration_ms", "upload_ack_ms", "first_event_ms", "prompt_get_ms",
		"variants_completed", "status", "quiet_delay_ms", "simulate_duration_ms",
		"simulate_interval_ms", "client_latency_ms", "latency_setting",
		"observation_started_at", "server_completed_at", "client_completed_at",
	}
	if needHeader {
		if err := w.Write(header); err != nil {
			return err
		}
	}

	dbReads := res.dbReads
	if res.mode == "sse" {
		dbReads = res.sseDBReads
	}
	row := []string{
		res.runID, res.condition, res.label, res.mode, fmt.Sprintf("%d", res.rep), res.jobID, res.image,
		fmt.Sprintf("%d", res.baselineVersion), fmt.Sprintf("%d", res.terminalVersion),
		fmt.Sprintf("%d", observationGets(res)), fmt.Sprintf("%d", res.sseStreamGets),
		fmt.Sprintf("%d", res.sseJobEvents), fmt.Sprintf("%d", res.sseHeartbeats),
		fmt.Sprintf("%d", res.versionsReceived), fmt.Sprintf("%d", res.versionGaps),
		fmt.Sprintf("%.1f", res.processingMS), fmt.Sprintf("%.1f", res.detectionMS),
		fmt.Sprintf("%d", dbReads), fmt.Sprintf("%.3f", res.streamSeconds),
		fmt.Sprintf("%d", res.peakActiveStreams), res.notes,
		fmt.Sprintf("%d", res.totalRequests), fmt.Sprintf("%d", res.initial200),
		fmt.Sprintf("%d", res.newer200), fmt.Sprintf("%d", res.unchanged200),
		fmt.Sprintf("%d", res.noContent204), fmt.Sprintf("%d", res.errors),
		fmt.Sprintf("%d", res.cancellations), fmt.Sprintf("%d", res.distinctNewSeen),
		fmt.Sprintf("%.4f", res.versionCoverage), fmt.Sprintf("%.1f", res.jobDurationMS),
		fmt.Sprintf("%.1f", res.uploadAckMS), fmt.Sprintf("%.2f", res.firstEventMS),
		fmt.Sprintf("%.2f", res.promptGetMS), fmt.Sprintf("%d", res.variantsCompleted), res.status,
		fmt.Sprintf("%d", res.quietDelayMS), fmt.Sprintf("%d", res.simulateDurMS),
		fmt.Sprintf("%d", res.simulateIntMS), fmt.Sprintf("%d", res.latencyMS), res.latencySetting,
		res.observationStartAt, res.serverCompletedAt, res.clientCompletedAt,
	}
	return w.Write(row)
}
