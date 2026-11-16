# Assessment 3 predictions (recorded before the measured runs)

These predictions were written before running `scripts/experiment-v3.sh`.
The comparison is **SSE versus version-based long polling**. Short polling is
retained but is not part of the required 18-run experiment.

Detection delay is the client receipt time of the terminal snapshot minus the
server log time immediately after the terminal transaction commits, joined by
job id. On loopback the network floor is sub-millisecond, so the measurement
isolates the observation mechanism.

## A — Quiet interval (30 s with no client-visible progress)

- **Long polling:** the held request stays open until the worker's first commit
  or the 20 s deadline. Expect ~5–7 observation GETs (claim, one deadline
  recheck, variant commits, completion), detection ≈ 1 ms, coverage 1.0.
- **SSE:** exactly **one** stream GET stays open for the whole run. Expect one
  initial snapshot, no job-update frames during the quiet interval, ~3
  heartbeat comments, then the variant and terminal frames. Detection ≈ 1 ms,
  coverage 1.0.
- **Prediction:** SSE and long polling should both be near the network floor;
  SSE's advantage here is observation GETs (1 vs ~6), not latency.

## B — Real progress (thumbnail → preview → display)

- **Long polling:** ~5–6 GETs (prompt snapshot, one per commit, completion),
  detection ≈ 1 ms, coverage 1.0.
- **SSE:** one stream GET; ~5–6 job-update frames from the same response,
  detection ≈ 1 ms, coverage 1.0.
- **Prediction:** the two methods should be statistically indistinguishable on
  detection and coverage. The graph should show two near-zero detection bars
  and a much lower GET bar for SSE.

## C — Frequent updates (10 s simulated phase at 200 ms, then real processing)

- **Long polling:** ~50–55 GETs because cost tracks the change rate: one
  request per committed version.
- **SSE:** one stream GET and ~50+ job-update frames inside it. Heartbeats
  remain low (one every 10 s).
- **Prediction:** SSE keeps GET count effectively constant while its event
  count rises with the update rate. Both methods should stay near the network
  floor for detection and near 1.0 coverage.
- **Prediction:** this is the condition where request count is most misleading.
  SSE's single GET does **not** mean fewer server resources: it holds one
  handler and one connection open for the whole run, sends one event per
  update, and performs one database read per wake-up. Long polling uses many
  short requests with no held handler between them. The experiment should be
  interpreted as a trade-off, not a total-cost win.

## Overall hypothesis

SSE and long polling should deliver comparable detection delay and version
coverage because both are driven by the same post-commit broadcast and both
send authoritative snapshots. SSE should win on observation GET count in every
condition and most clearly under frequent updates. That lower request count is
not, by itself, evidence of lower total resource use: SSE pays with a held
handler, stream duration, per-update event traffic, and subscriber bookkeeping.
Long polling remains reasonable when updates are sparse and holding a
connection open is undesirable.
