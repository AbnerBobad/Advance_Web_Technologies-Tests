package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"imagelab/internal/data"
)

const (
	// sseHeartbeatInterval is how often an idle stream receives a comment
	// frame. Heartbeats keep intermediaries from treating the stream as dead
	// and refresh the connection's write deadline (SSE-08).
	sseHeartbeatInterval = 10 * time.Second
	// sseWriteDeadline is refreshed before every frame. The server-wide
	// WriteTimeout is 30s, which is shorter than condition A's quiet
	// interval; without this refresh a valid stream would be torn down
	// mid-job. A heartbeat every 10s keeps the deadline ahead of the stream.
	sseWriteDeadline = 30 * time.Second
)

// sseStreamStarted records one more active stream and updates the peak.
func (app *application) sseStreamStarted() (active, peak int64) {
	active = app.sseActive.Add(1)
	for {
		peak = app.ssePeak.Load()
		if active <= peak || app.ssePeak.CompareAndSwap(peak, active) {
			return active, app.ssePeak.Load()
		}
	}
}

// sseStreamEnded records that one stream has closed.
func (app *application) sseStreamEnded() int64 {
	return app.sseActive.Add(-1)
}

// jobEventsHandler serves GET /v1/jobs/{id}/events for EventSource.
//
// The response is one long-lived text/event-stream. It starts with the
// current complete snapshot, then sends another job-update frame whenever a
// newer version has committed, and ends after the terminal snapshot. A
// notification is only a signal to re-read PostgreSQL; the payload is always
// a freshly read snapshot (SYNC-04, SSE-05).
func (app *application) jobEventsHandler(w http.ResponseWriter, r *http.Request) {
	publicID := r.PathValue("id")
	streamStart := time.Now()

	// SYNC-03: subscribe before the first read. The registration is held
	// across the snapshot write and the wait, so a commit that lands between
	// the read and the wait closes the channel we already hold and cannot be
	// missed. A broadcast never blocks on registrations; it closes and
	// removes the signal, so holding one during network I/O is cheap.
	sig := app.broadcaster.subscribe(publicID)

	// SSE-02: resolve the job before writing any stream headers. An unknown
	// job is an ordinary 404 response, not an empty 200 stream.
	job, err := app.models.Jobs.GetByPublicID(publicID)
	if err != nil {
		app.broadcaster.release(publicID, sig)
		if errors.Is(err, data.ErrRecordNotFound) {
			app.notFoundResponse(w, r)
			return
		}
		app.serverErrorResponse(w, r, err)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		app.broadcaster.release(publicID, sig)
		app.serverErrorResponse(w, r, errors.New("streaming is not supported by this connection"))
		return
	}

	// SSE-03: stream headers must be set before the first body byte. The
	// proxy-buffering hint is ignored where the deployment has no proxy.
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	controller := http.NewResponseController(w)

	// Observability for the experiment and for multi-tab demonstrations: the
	// server logs how many streams are active when this one starts and ends.
	active, peak := app.sseStreamStarted()
	dbReads := 1
	eventsSent := 0
	heartbeats := 0
	endReason := "client_disconnected"

	defer func() {
		remaining := app.sseStreamEnded()
		app.logger.Info("sse stream ended",
			"job_id", publicID,
			"end_reason", endReason,
			"events_sent", eventsSent,
			"heartbeats", heartbeats,
			"db_reads", dbReads,
			"duration_ms", time.Since(streamStart).Milliseconds(),
			"active_streams", remaining,
			"peak_active_streams", app.ssePeak.Load())
	}()

	app.logger.Info("sse stream started",
		"job_id", publicID,
		"version", job.Version,
		"status", job.Status,
		"active_streams", active,
		"peak_active_streams", peak)

	// writeFrame writes one complete SSE frame and flushes it. Flush makes
	// the frame visible to the client while the response stays open; it does
	// not end the response (SSE-06). The frame deliberately has no id field:
	// this version sends snapshots, not replayable event history (SSE-09).
	writeFrame := func(event string, data []byte) bool {
		_ = controller.SetWriteDeadline(time.Now().Add(sseWriteDeadline))
		if _, err := w.Write([]byte("event: " + event + "\ndata: ")); err != nil {
			return false
		}
		if _, err := w.Write(data); err != nil {
			return false
		}
		if _, err := w.Write([]byte("\n\n")); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	writeHeartbeat := func() bool {
		_ = controller.SetWriteDeadline(time.Now().Add(sseWriteDeadline))
		if _, err := w.Write([]byte(": heartbeat\n\n")); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	// sendSnapshot builds the shared public representation and frames it as
	// one complete job-update event. The caller decides whether it is newer
	// than the stream's cursor.
	sendSnapshot := func(ctx context.Context, current *data.Job) bool {
		response, err := app.jobSnapshot(ctx, current)
		if err != nil {
			endReason = "snapshot_error"
			return false
		}
		payload, err := json.Marshal(response)
		if err != nil {
			endReason = "snapshot_error"
			return false
		}
		if !writeFrame("job-update", payload) {
			endReason = "client_gone"
			return false
		}
		eventsSent++
		return true
	}

	// SSE-04: the current snapshot is sent immediately, before waiting for
	// anything. A client that reconnects therefore recovers the latest state
	// without replaying intermediate versions (UI-05).
	if !sendSnapshot(r.Context(), job) {
		app.broadcaster.release(publicID, sig)
		return
	}
	if isTerminal(job.Status) {
		app.broadcaster.release(publicID, sig)
		endReason = "terminal"
		return
	}

	lastVersion := job.Version
	heartbeat := time.NewTicker(sseHeartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-sig.ch:
			// A committed change woke us. Release the closed registration,
			// then subscribe before reading so a commit during the read
			// closes the new channel and cannot be missed (SYNC-03).
			app.broadcaster.release(publicID, sig)
			sig = app.broadcaster.subscribe(publicID)

			latest, err := app.models.Jobs.GetByPublicID(publicID)
			dbReads++
			if err != nil {
				app.broadcaster.release(publicID, sig)
				if errors.Is(err, data.ErrRecordNotFound) {
					endReason = "job_missing"
				} else {
					endReason = "read_error"
				}
				return
			}

			if latest.Version > lastVersion {
				// Keep the registration across the write: a commit during
				// the write closes it, so the next select returns at once
				// instead of waiting for a later change.
				if !sendSnapshot(r.Context(), latest) {
					app.broadcaster.release(publicID, sig)
					return
				}
				lastVersion = latest.Version
				// SSE-07: the terminal snapshot is the last frame; then the
				// server ends the stream so the browser can close EventSource.
				if isTerminal(latest.Status) {
					app.broadcaster.release(publicID, sig)
					endReason = "terminal"
					return
				}
			}
		case <-heartbeat.C:
			// Keep the same registration while writing: an idle stream
			// needs no database read to send a keep-alive comment.
			if !writeHeartbeat() {
				app.broadcaster.release(publicID, sig)
				endReason = "client_gone"
				return
			}
			heartbeats++
		case <-r.Context().Done():
			// Only observation ends here; the background worker and the job
			// are unaffected (SYNC-07).
			app.broadcaster.release(publicID, sig)
			endReason = "client_disconnected"
			return
		}
	}
}
