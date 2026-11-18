# Version 3 Demo — Script and Recording (Assessment 3)

A 10–15 minute narrated walkthrough of ImageLab Version 3: Server-Sent Events
(SSE) beside short and long polling. Record the screen (terminal + browser),
narrate each section, and upload as an **unlisted** video or provide a
shareable link.

> **Video link:** _TODO — paste the unlisted video URL here before submitting._

## Before recording

1. Terminal A — start the API with a visible phase so the stream shows updates:

   ```sh
   make db/migrations/up            # schema version should stay 5
   go run ./cmd/api -simulate-duration=10s -simulate-interval=500ms
   ```

2. Terminal B — ready-to-paste commands:

   ```sh
   BASE=http://localhost:4000
   ```

3. Browser — open <http://localhost:4000>, zoom to ~125%, DevTools → Network
   with **Fetch/XHR** visible. Keep `experiments/v3/summary.csv` and the three
   SVG charts open in another tab or editor.
4. Have `experiments/v3/runs.csv` and `experiments/v3/raw/` available for the
   evidence section.

## Timeline

### 0:00–1:30 — Scope and preserved contracts

- Version 3 adds a **third observation mode**, not a new pipeline.
- Unchanged: `POST /v1/images` → `202 Accepted` + `status_url`; durable
  PostgreSQL jobs; one in-process worker; four states; three variants; short and
  long polling.
- New: `GET /v1/jobs/{id}/events` — one long-lived response carrying complete
  snapshots.
- State that the schema did **not** change: migration version 5 already has
  `version`, `stage`, `variants_completed`, and `simulation_step`.

### 1:30–3:00 — The SSE frame on the wire

```sh
JOB=$(curl -s -X POST "$BASE/v1/images" -F "file=@/tmp/sse-assets/photo.png" \
      | grep -o '"job_id": "[^"]*"' | cut -d'"' -f4)
curl -N -D - "$BASE/v1/jobs/$JOB/events"
```

Point at the terminal:

- `HTTP/1.1 200 OK`
- `Content-Type: text/event-stream; charset=utf-8`
- `Cache-Control: no-cache`
- `event: job-update` + `data: { ...complete snapshot... }` + blank line.
- No `id:` field — say why: this is snapshots, not replayable history.
- The request stays **pending** after the first frame; that is the whole idea.
- Send an unknown id first (`/v1/jobs/00000000-.../events`) and show a plain
  `404` with no stream.

Explain: the server keeps the handler, the response writer, and the subscriber
registration open. `Flush` pushes the current frame to the client without ending
the response.

### 3:00–6:00 — Browser lifecycle under SSE

- Upload a fixture, select **Server-Sent Events**, click **Process image**.
- DevTools → Network: exactly **one** `events` request, pending for the whole
  job; open **EventStream** and show the arriving `job-update` frames.
- Job card: mode chip **Server-Sent Events**, version counter, event count,
  stream GET count, stop button.
- Show the stream ending by itself at the terminal snapshot; the browser calls
  `EventSource.close()`.
- Click **Stop observing** on a second, longer job: stream closes, panel says
  *Observation stopped*, and the job keeps running. Click **Resume
  observation**: one new `EventSource`, immediate current snapshot (version
  jumps ahead), then completion.
- Optional reconnect proof: hard-kill the API while the stream is open. The card
  shows *Reconnecting to the job stream*, **not** *failed*. Restart the API; the
  page opens a fresh stream and the initial snapshot restores the current
  version.

### 6:00–8:30 — The fan-out: two tabs, one commit

- Submit one job in tab 1; open tab 2 at the same `status_url` with SSE selected
  (or run two `curl -N` streams).
- Show both receive the same `version` sequence.
- Explain: the worker commits, then closes the broadcaster's per-job signal;
  every waiting handler wakes, **re-reads PostgreSQL**, and writes its own frame.
  Notification follows commit; the database stays authoritative.
- Server log line: `sse stream ended ... events_sent=N db_reads=M
  active_streams=0 peak_active_streams=2`. Peak 2 proves both observers were
  registered and notified. Reading the log's `db_reads` shows the stream re-read
  per wake-up rather than per timer tick.
- Emphasise: the worker never knows which observers exist; closing a stream does
  not cancel the job.

### 8:30–9:30 — Long polling beside SSE

Switch the **same job** to **Long polling** (or submit a new one) and show in
DevTools:

- one prompt `GET` returns immediately;
- then a sequence of held `GET ...?after=<v>&wait=20` requests, each returning
  the next version or a `204` at the holding deadline;
- request count rises with the change rate.

Keep the contrast concrete: same snapshots, same broadcast, same database —
**one open response** versus **a new request per change**.

### 9:30–12:30 — The measured comparison

Show `experiments/v3-predictions.md` first: predictions were recorded before the
runs.

1. **Run-by-run table** — `experiments/v3/runs.csv`, 18 rows, one observer per
   run. Point out the columns: `observation_gets`, `sse_stream_gets`,
   `job_events_received`, `heartbeats`, `detection_ms`, `versions_received`,
   `version_gaps`, `observation_db_reads`, `stream_seconds`,
   `peak_active_streams`.
2. **Summary table** — `experiments/v3/summary.csv`: median with min–max per
   condition and method.
3. **Graph 1** — `chart-gets.svg`: observation GETs. SSE stays at 1 in every
   condition; long polling scales with the change rate.
4. **Graph 2** — `chart-detection.svg`: detection delay. Both methods sit near
   the loopback floor; the bars are close and the whiskers show spread.
5. **Supporting graph** — `chart-coverage.svg`: both methods deliver all
   committed versions; gaps are reported but snapshots make them recoverable.

Read the numbers aloud from the actual summary table, condition by condition.

### 12:30–14:30 — Interpretation, limits, recommendation

- **What the graphs show that the table hides:** the GET graph makes the
  *scaling shape* obvious — SSE is flat at one stream GET while long polling's
  request count tracks update frequency. The detection graph shows how tightly
  both methods cluster at the network floor, which is easy to miss in a table of
  similar numbers.
- **What fewer requests does not prove:** SSE is not automatically cheaper. It
  holds one handler and one connection for the entire run, sends one event per
  committed version, performs one database read per wake-up, and keeps
  subscriber bookkeeping. Long polling uses many short requests with no held
  handler between them. Point at `stream_seconds`, `observation_db_reads`, and
  `peak_active_streams` for the SSE side.
- **Limits:** loopback only; three repetitions per cell; one observer during the
  measured runs; one worker; condition C is labelled *simulated* progress;
  connection count was not measured (HTTP request count is not TCP connection
  count); no WAN latency, packet loss, or proxy buffering.
- **Recommendation:** use **SSE** when the client should watch a job to
  completion and updates may be frequent — one stream, near-floor detection,
  full coverage, no request storm. Keep **long polling** for sparse updates or
  environments where holding a response open is undesirable, and keep the
  prompt `GET` as the immediate, stateless read. All three share the same
  authoritative snapshots, so switching is safe.

### 14:30–15:00 — Evidence map and close

| Deliverable | Location |
| :-- | :-- |
| SSE handler | `cmd/api/events.go` |
| Shared snapshot | `cmd/api/jobs.go` (`jobSnapshot`) |
| Browser lifecycle | `frontend/app.js` (`startEventStream`), `frontend/render.js` |
| Run-by-run data | `experiments/v3/runs.csv` |
| Summary table | `experiments/v3/summary.csv` |
| Graphs | `experiments/v3/chart-gets.svg`, `chart-detection.svg`, `chart-coverage.svg` |
| Raw client logs | `experiments/v3/raw/*.jsonl` |
| Server logs | `experiments/v3/logs/*.log` |
| Lifecycle checks | `experiments/v3-lifecycle/` (13/13, two-observer log) |
| Pre-run predictions | `experiments/v3-predictions.md` |
| Report | `REPORT-V3.md` |
| Slides | `PRESENTATION-V3.md` (Marp; speaker notes and timings included) |
| Reproduction | `make experiment-v3`, `make sse-smoke` |

## Likely in-class questions

**What does the server keep open, and what does `Flush` do?**
The server keeps the HTTP response writer, the handler goroutine, and the job's
subscriber registration open. `Flush` writes the buffered frame to the client
immediately but leaves the response open, so the next frame can follow later.

**How does a committed job version become an SSE event for every connected tab?**
The worker commits the new version, then closes that job's signal channel in the
broadcaster. Every handler waiting on the channel wakes, releases its
registration, re-reads PostgreSQL, and writes the newer complete snapshot to its
own response. Each tab has its own handler and its own response.

**What happens when a connection drops while the worker is still running?**
Only observation ends. The handler's request context is cancelled, it releases
its broadcaster registration and exits; the worker keeps processing and commits
the job normally. The experiment's disconnect check confirms the job still
reaches `completed`.

**Why can a reconnecting client receive the latest snapshot without replaying
every intermediate version?**
The stream carries snapshots, not deltas, and has no `id:`/replay contract. A new
stream starts with the current committed snapshot, so it is correct even after
missing versions. A version gap is not data loss; the latest state is complete.

**What did the graphs show that was harder to see in the tables, and what do the
results not establish?**
The GET graph shows SSE's flat single-request profile versus long polling's
frequency-driven growth, and the detection graph shows both methods clustered at
the loopback floor. The results do not establish total resource cost: held
handlers, connection lifetime, event traffic, and per-update database reads are
not free. They also do not generalise beyond loopback, three repetitions, one
observer, and one worker.
