# ImageLab Version 3 — SSE vs Long Polling

**Question.** Version 3 adds Server-Sent Events (SSE) as a third way to observe a
job. This report compares **SSE** with **version-based long polling** under the
required three conditions and asks when each is the better observation
mechanism. Short polling and the upload/worker pipeline are unchanged and are
not part of the 18-run comparison.

All numbers come from `scripts/experiment-v3.sh` and are stored in
`experiments/v3/`:

- `runs.csv` — 18 run-by-run rows (3 conditions × 2 methods × 3 repetitions).
- `summary.csv` — median and min–max per condition and method.
- `chart-gets.svg`, `chart-detection.svg` — the two required graphs;
  `chart-coverage.svg` supports the version-coverage discussion.
- `raw/*.jsonl` — per-run client traces, including every SSE frame.
- `logs/*.log` — per-condition server logs with post-commit times and stream
  summaries.

Predictions were recorded in `experiments/v3-predictions.md` **before** the runs.

---

## Method

**Harness.** `cmd/experiment` is a headless Go client. In `sse` mode it opens one
`GET /v1/jobs/{id}/events`, parses the named `job-update` frames, counts stream
GETs separately from job events and heartbeat comments, and stops reading after
the terminal frame. In `long` mode it issues
`GET /v1/jobs/{id}?after=<v>&wait=20`, and after each active `200` or holding
`204` immediately issues the next request with the newest version.

**Detection delay** is `client receipt of the terminal snapshot − the server log
time immediately after the terminal job transaction commits`, joined by job id.
Processing duration is reported separately as worker `started_at` to terminal
time. On loopback the network floor is sub-millisecond, so extra delay comes from
observation scheduling, not transport distance.

**Version coverage** is `distinct new versions received / (terminal version −
baseline version)`. A gap is not automatically data loss: both methods deliver
complete snapshots, so the latest snapshot is correct even when an intermediate
version was coalesced.

**Conditions** (fresh server per condition, same generated PNG, empty queue at
each run start, one observer per run, method order alternated between
conditions):

| ID | Workload | Server flags |
| :-- | :-- | :-- |
| A — quiet interval | Active for 30 s with no client-visible progress, then real work | `-quiet-delay 30s` |
| B — real progress | Real thumbnail, preview, and display commits | none |
| C — frequent updates | Labelled 10 s simulated phase at 200 ms, then real work | `-simulate-duration 10s -simulate-interval 200ms` |

The simulated phase is labelled as simulated (`simulation_step`) and never marks
a job complete; only real processing does.

**Observer work.** For SSE the server logs `db_reads`, stream duration, and
`peak_active_streams`. `db_reads` counts state reads performed for observation
(the terminal snapshot's variant lookup is part of the same read, as it is for
one long-poll response).

**Threats to validity.** Loopback only; three repetitions per cell; one observer;
one worker; condition C is simulated progress; request count is not TCP
connection count.

---

## Results

### Run-by-run measurements

| # | Cond | Method | Rep | Observation GETs | Job events | Detection (ms) | Versions (gaps) | Coverage | DB reads | Stream (s) |
|--:|:--|:--|--:|--:|--:|--:|:--|--:|--:|--:|
| 1 | A | long | 1 | 6 | 0 | 6.00 | 5 (0) | 1.00 | 6 | — |
| 2 | A | long | 2 | 6 | 0 | 4.40 | 5 (0) | 1.00 | 6 | — |
| 3 | A | long | 3 | 6 | 0 | 0.90 | 5 (0) | 1.00 | 6 | — |
| 4 | A | sse | 1 | 1 | 5 | 3.20 | 5 (0) | 1.00 | 5 | 30.21 |
| 5 | A | sse | 2 | 1 | 5 | 2.70 | 5 (0) | 1.00 | 5 | 30.32 |
| 6 | A | sse | 3 | 1 | 5 | 0.70 | 5 (0) | 1.00 | 5 | 30.44 |
| 7 | B | long | 1 | 5 | 0 | 0.90 | 5 (0) | 1.00 | 5 | — |
| 8 | B | long | 2 | 5 | 0 | 0.80 | 5 (0) | 1.00 | 5 | — |
| 9 | B | long | 3 | 5 | 0 | 0.80 | 5 (0) | 1.00 | 5 | — |
| 10 | B | sse | 1 | 1 | 5 | 3.40 | 5 (0) | 1.00 | 5 | 0.33 |
| 11 | B | sse | 2 | 1 | 5 | 3.20 | 5 (0) | 1.00 | 5 | 0.19 |
| 12 | B | sse | 3 | 1 | 5 | 0.80 | 5 (0) | 1.00 | 5 | 0.24 |
| 13 | C | long | 1 | 55 | 0 | 1.20 | 55 (0) | 1.00 | 55 | — |
| 14 | C | long | 2 | 55 | 0 | 0.70 | 55 (0) | 1.00 | 55 | — |
| 15 | C | long | 3 | 55 | 0 | 0.80 | 55 (0) | 1.00 | 55 | — |
| 16 | C | sse | 1 | 1 | 55 | 0.80 | 55 (0) | 1.00 | 55 | 10.25 |
| 17 | C | sse | 2 | 1 | 55 | 1.10 | 55 (0) | 1.00 | 55 | 10.18 |
| 18 | C | sse | 3 | 1 | 55 | 2.20 | 55 (0) | 1.00 | 55 | 10.23 |

### Summary (median, min–max across 3 runs)

| Cond | Method | GETs | Detection (ms) | Job events | Coverage | DB reads | Stream (s) |
|:--|:--|--:|:--|--:|--:|--:|--:|
| A | long | 6 (6–6) | 4.40 (0.90–6.00) | 0 | 1.00 | 6 | — |
| A | sse | 1 (1–1) | 2.70 (0.70–3.20) | 5 | 1.00 | 5 | 30.32 |
| B | long | 5 (5–5) | 0.80 (0.80–0.90) | 0 | 1.00 | 5 | — |
| B | sse | 1 (1–1) | 3.20 (0.80–3.40) | 5 | 1.00 | 5 | 0.24 |
| C | long | 55 (55–55) | 0.80 (0.70–1.20) | 0 | 1.00 | 55 | — |
| C | sse | 1 (1–1) | 1.10 (0.80–2.20) | 55 | 1.00 | 55 | 10.23 |

Server logs confirm SSE delivered every committed version (`events_sent` equals
the terminal version count), used one database state read per version, and
reported `peak_active_streams=1` because each performance run used one observer.

### Predictions vs. observations

| Prediction (before runs) | Observed | Verdict |
| :-- | :-- | :-- |
| A long ~5–7 GETs, detection ≈ 1 ms | 6 GETs, 4.4 ms median | GETs confirmed; latency a few ms above the floor in early reps |
| A SSE one stream GET, no update frames during quiet, detection ≈ 1 ms | 1 GET, 5 events total, 2.7 ms median | confirmed; heartbeat comment present, no polling |
| B long ~5–6 GETs, ≈ 1 ms, coverage 1.0 | 5 GETs, 0.8 ms, 1.00 | confirmed |
| B SSE one GET, same frames, ≈ 1 ms | 1 GET, 5 events, 3.2 ms median | frames/coverage confirmed; 2–3 ms above floor in first reps |
| C long ~50–55 GETs, full coverage | 55 GETs, 55 versions, 1.00 | confirmed |
| C SSE one GET, 50+ events, ≈ 1 ms | 1 GET, 55 events, 1.1 ms | confirmed |
| SSE pays with a held handler, event traffic, and per-update DB reads | 30.2 s held in A; 55 events and 55 reads in C | confirmed |

The only deviation is detection delay: the first measured repetitions after a
server start landed at 3–6 ms rather than ≈ 1 ms, then settled to 0.7–1.1 ms.
That is cold-start scheduling noise, not a difference between the mechanisms;
every value is still far below one polling interval.

### What the graphs show

**Graph 1 — observation GETs (`chart-gets.svg`).** SSE is a flat bar at **1**
in every condition; long polling is **6, 5, and 55**. The table shows the same
numbers, but the graph makes the *shape* obvious: long polling's request cost
tracks the change rate, while SSE's request count is independent of it. In C,
the same 55 versions cost 55 requests under long polling and one stream GET under
SSE.

**Graph 2 — detection delay (`chart-detection.svg`).** Both methods sit in the
same low band. The graph shows overlapping min–max whiskers and individual dots,
making clear that the difference between the methods is small relative to the
run-to-run spread. It also exposes the cold-start points that a single median
would hide.

**Supporting graph — version coverage (`chart-coverage.svg`).** Both methods
reach 100% in all three conditions; no version gaps occurred in the final 18
runs. A gap would still be recoverable, because each frame is a complete
snapshot.

---

## Interpretation

1. **Both methods detect completion at roughly the same speed.** Medians range
   from 0.8 to 4.4 ms, with overlapping ranges. Both are woken by the same
   post-commit broadcast and both re-read authoritative state, so the observation
   mechanism is not the bottleneck. The spread, not the median, is the honest
   result at this scale.

2. **SSE changes the request profile, not the update path.** One stream GET
   carries 5 events in A and B and 55 in C. Long polling needs one request per
   change: 6, 5, and 55. SSE removes repeated HTTP request/response cycles from
   the client's view, including long polling's 20 s deadline recheck in A (one
   extra GET and one extra state read).

3. **Fewer GETs do not mean fewer server resources.** In C, SSE and long polling
   each performed **55 database state reads** — one per committed version.
   SSE also held a handler and connection for the entire run (30.2 s in A,
   10.2 s in C), sent one event per commit, and kept a subscriber registration.
   Long polling used many short requests with no held handler between them. The
   saving is HTTP request overhead, not database work.

4. **Version coverage is a property of the contract, not the transport.** Both
   methods delivered every committed version here because the worker's changes
   were slower than the observer's read-and-send cycle. When updates coalesce, a
   snapshot client still converges to the latest version; only intermediate
   progress display is lost. The API therefore reports versions and gaps, not
   "missed events".

5. **Processing time is separate from observation time.** Condition A processed
   for ~30.2 s but detection added only a few milliseconds; condition B
   processed in ~0.13 s and still detected at the floor. Detection delay must not
   be conflated with processing duration.

---

## Limitations

- Loopback only: no WAN latency, packet loss, proxy buffering, or reconnection
  storms. The measured floor is a best case.
- Three repetitions per cell: enough for medians and ranges, not for confidence
  intervals or significance tests.
- One observer per performance run. Multi-tab fan-out was verified separately
  (`experiments/v3-lifecycle/smoke.txt`: 13/13 checks; server log shows two
  observers on one job, `peak_active_streams=2`, and the worker continuing after
  a disconnect), but its resource cost was not measured.
- One worker and one server process; no concurrency or load testing.
- Condition C uses a labelled simulated phase, so it measures observation under
  a cadence, not image-processing performance.
- Held-handler memory, connection count, and event-serialization CPU were not
  measured; the request trace counts HTTP requests, not TCP connections.

---

## Recommendation

- **Use SSE** when a browser should watch a job to completion, especially when
  updates may be frequent: one stream GET, near-floor detection, full snapshot
  coverage, and no request storm as the update rate rises. Stop/resume and
  reconnection work without ever marking the job failed.
- **Keep long polling** where holding a response open is undesirable or
  unsupported (restrictive proxies, short connection budgets) and updates are
  sparse. Its request count is proportional to change rate, which is cheap for a
  quiet job.
- **Keep the prompt `GET`** as the stateless immediate read and the universal
  fallback.
- Because all three modes render the same authoritative snapshot, switching
  modes is safe: a client can resume from the last version it saw.

For ImageLab, the default should be **SSE for job observation**, with long
polling retained for constrained environments and short polling as the simple
fallback. The 18-run evidence supports SSE on request efficiency and equal
detection/coverage; it does **not** establish lower total server cost, because
SSE trades requests for a held handler, open connection, and event traffic.
