// Command sse-summary reads a Version 3 runs.csv (SSE versus long polling)
// and writes a per-condition summary plus labelled SVG charts. Each chart
// shows every repetition as a point, the median as a bar, and the min-max
// range as a whisker, so the graph and the table describe the same data.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

var (
	conditionOrder = []string{"A", "B", "C"}
	methodOrder    = []string{"long", "sse"}
	methodLabel    = map[string]string{"long": "Long polling", "sse": "SSE"}
	methodColor    = map[string]string{"long": "#e0975a", "sse": "#4fae9b"}
	conditionLabel = map[string]string{
		"A": "A · Quiet 30 s",
		"B": "B · Real progress",
		"C": "C · 10 s @ 200 ms",
	}
)

type run struct {
	runID        string
	condition    string
	label        string
	mode         string
	repetition   int
	gets         float64
	detectionMS  float64
	events       float64
	coverage     float64
	processingMS float64
	dbReads      float64
	streamSecs   float64
	peakStreams  float64
	values       int
	gaps         int
}

type group struct {
	condition string
	label     string
	mode      string
	runs      []run
}

func main() {
	var csvPath, outDir string
	flag.StringVar(&csvPath, "csv", "experiments/v3/runs.csv", "runs CSV")
	flag.StringVar(&outDir, "outdir", "experiments/v3", "output directory")
	flag.Parse()

	runs, err := readRuns(csvPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	groups := groupRuns(runs)
	if err := writeSummary(filepath.Join(outDir, "summary.csv"), groups); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	charts := []struct {
		file     string
		title    string
		yLabel   string
		value    func(run) float64
		format   func(float64) string
		fixedMax float64
	}{
		{
			file:   "chart-gets.svg",
			title:  "Observation GETs by condition and method",
			yLabel: "Observation HTTP GETs per run",
			value:  func(r run) float64 { return r.gets },
			format: formatInt,
		},
		{
			file:   "chart-detection.svg",
			title:  "Completion-detection delay by condition and method",
			yLabel: "Detection delay (ms)",
			value:  func(r run) float64 { return r.detectionMS },
			format: formatMS,
		},
		{
			file:     "chart-coverage.svg",
			title:    "Version coverage by condition and method",
			yLabel:   "Versions received / versions committed (%)",
			value:    func(r run) float64 { return r.coverage * 100 },
			format:   func(v float64) string { return fmt.Sprintf("%.0f%%", v) },
			fixedMax: 100,
		},
	}

	for _, c := range charts {
		svg := chart(c.title, c.yLabel, groups, c.value, c.format, c.fixedMax)
		if err := os.WriteFile(filepath.Join(outDir, c.file), []byte(svg), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	}

	fmt.Println("wrote summary.csv and 3 charts to", outDir)
}

func readRuns(path string) ([]run, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) < 2 {
		return nil, fmt.Errorf("no data rows in %s", path)
	}

	idx := map[string]int{}
	for i, name := range records[0] {
		idx[name] = i
	}
	get := func(rec []string, key string) string {
		i, ok := idx[key]
		if !ok || i >= len(rec) {
			return ""
		}
		return rec[i]
	}

	out := make([]run, 0, len(records)-1)
	for _, rec := range records[1:] {
		mode := get(rec, "mode")
		if mode != "long" && mode != "sse" {
			continue
		}
		condition := get(rec, "condition")
		label := get(rec, "condition_label")
		if label == "" {
			label = conditionLabel[condition]
		}
		out = append(out, run{
			runID:        get(rec, "run_id"),
			condition:    condition,
			label:        label,
			mode:         mode,
			repetition:   atoi(get(rec, "repetition")),
			gets:         atof(get(rec, "observation_gets")),
			detectionMS:  atof(get(rec, "detection_ms")),
			events:       atof(get(rec, "job_events_received")),
			coverage:     atof(get(rec, "version_coverage")),
			processingMS: atof(get(rec, "processing_ms")),
			dbReads:      atof(get(rec, "observation_db_reads")),
			streamSecs:   atof(get(rec, "stream_seconds")),
			peakStreams:  atof(get(rec, "peak_active_streams")),
			values:       atoi(get(rec, "versions_received")),
			gaps:         atoi(get(rec, "version_gaps")),
		})
	}
	return out, nil
}

func groupRuns(runs []run) []group {
	index := map[string]*group{}
	for _, r := range runs {
		key := r.condition + "|" + r.mode
		g, ok := index[key]
		if !ok {
			g = &group{condition: r.condition, label: r.label, mode: r.mode}
			index[key] = g
		}
		if g.label == "" {
			g.label = r.label
		}
		g.runs = append(g.runs, r)
	}

	var out []group
	for _, cond := range conditionOrder {
		for _, mode := range methodOrder {
			if g, ok := index[cond+"|"+mode]; ok {
				out = append(out, *g)
			}
		}
	}
	return out
}

func writeSummary(path string, groups []group) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"condition", "condition_label", "mode", "n",
		"observation_gets_median", "observation_gets_min", "observation_gets_max",
		"detection_ms_median", "detection_ms_min", "detection_ms_max",
		"job_events_median", "job_events_min", "job_events_max",
		"version_coverage_median", "processing_ms_median",
		"observation_db_reads_median", "stream_seconds_median",
		"peak_active_streams_max", "version_gaps_total",
	}
	if err := w.Write(header); err != nil {
		return err
	}

	for _, g := range groups {
		var gets, det, events, cov, proc, db, stream []float64
		peak := 0.0
		gaps := 0
		for _, r := range g.runs {
			gets = append(gets, r.gets)
			det = append(det, r.detectionMS)
			events = append(events, r.events)
			cov = append(cov, r.coverage)
			proc = append(proc, r.processingMS)
			db = append(db, r.dbReads)
			stream = append(stream, r.streamSecs)
			if r.peakStreams > peak {
				peak = r.peakStreams
			}
			gaps += r.gaps
		}
		rec := []string{
			g.condition, g.label, g.mode, strconv.Itoa(len(g.runs)),
			one(median(gets)), one(minOf(gets)), one(maxOf(gets)),
			two(median(det)), two(minOf(det)), two(maxOf(det)),
			one(median(events)), one(minOf(events)), one(maxOf(events)),
			four(median(cov)), one(median(proc)),
			one(median(db)), two(median(stream)),
			one(peak), strconv.Itoa(gaps),
		}
		if err := w.Write(rec); err != nil {
			return err
		}
	}
	return nil
}

// ---------- SVG chart ----------

func chart(title, yLabel string, groups []group, value func(run) float64, format func(float64) string, fixedMax float64) string {
	const (
		w, h   = 900.0, 500.0
		left   = 90.0
		right  = 30.0
		top    = 74.0
		bottom = 110.0
	)
	plotW := w - left - right
	plotH := h - top - bottom

	// Group keyed by condition|mode for lookup.
	byKey := map[string]group{}
	maxV := 0.0
	for _, g := range groups {
		byKey[g.condition+"|"+g.mode] = g
		for _, r := range g.runs {
			if v := value(r); v > maxV {
				maxV = v
			}
		}
	}
	axisMax := fixedMax
	if axisMax <= 0 {
		axisMax = niceCeil(maxV * 1.15)
	}
	if axisMax <= 0 {
		axisMax = 1
	}
	y := func(v float64) float64 { return top + plotH - (v/axisMax)*plotH }

	slot := plotW / float64(len(conditionOrder))
	barW := 30.0
	barGap := 14.0

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" width="%.0f" height="%.0f" viewBox="0 0 %.0f %.0f" font-family="system-ui, sans-serif">`, w, h, w, h)
	b.WriteString(`<rect width="100%" height="100%" fill="#ffffff"/>`)
	fmt.Fprintf(&b, `<text x="%.0f" y="34" font-size="19" font-weight="600" fill="#1d2430">%s</text>`, left, esc(title))
	fmt.Fprintf(&b, `<text x="%.0f" y="56" font-size="12" fill="#667085">Bars are medians; whiskers are min–max; dots are the three individual runs.</text>`, left)

	// Horizontal grid and y-axis ticks.
	const ticks = 5
	for i := 0; i <= ticks; i++ {
		v := axisMax * float64(i) / ticks
		yy := y(v)
		fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="#e6e8ec" stroke-width="1"/>`, left, yy, w-right, yy)
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" font-size="11" fill="#667085" text-anchor="end">%s</text>`, left-10, yy+4, format(v))
	}
	fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="#98a2b3" stroke-width="1.2"/>`, left, top+plotH, w-right, top+plotH)
	fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="#98a2b3" stroke-width="1.2"/>`, left, top, left, top+plotH)
	fmt.Fprintf(&b, `<text x="22" y="%.1f" font-size="12" fill="#475467" text-anchor="middle" transform="rotate(-90 22 %.1f)">%s</text>`, top+plotH/2, top+plotH/2, esc(yLabel))

	for gi, cond := range conditionOrder {
		center := left + slot*float64(gi) + slot/2

		// Condition tick label.
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" font-size="13" font-weight="600" fill="#1d2430" text-anchor="middle">%s</text>`,
			center, top+plotH+24, esc(cond))
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" font-size="11" fill="#667085" text-anchor="middle">%s</text>`,
			center, top+plotH+42, esc(strings.TrimSpace(strings.SplitN(conditionLabel[cond], "·", 2)[1])))

		for mi, mode := range methodOrder {
			g, ok := byKey[cond+"|"+mode]
			if !ok || len(g.runs) == 0 {
				continue
			}
			values := make([]float64, 0, len(g.runs))
			for _, r := range g.runs {
				values = append(values, value(r))
			}
			lo, hi := minOf(values), maxOf(values)
			med := median(values)
			x := center - (barW+barGap)/2 + float64(mi)*(barW+barGap)
			color := methodColor[mode]

			// Median bar.
			fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" fill="%s" fill-opacity="0.85" rx="2"/>`,
				x-barW/2, y(med), barW, top+plotH-y(med), color)
			// Min-max whisker.
			fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="#344054" stroke-width="1.3"/>`, x, y(lo), x, y(hi))
			fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="#344054" stroke-width="1.3"/>`, x-6, y(hi), x+6, y(hi))
			fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="#344054" stroke-width="1.3"/>`, x-6, y(lo), x+6, y(lo))

			// Individual repetitions.
			for ri, v := range values {
				jitter := []float64{0, -8, 8}[ri%3]
				fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="3.2" fill="#ffffff" stroke="%s" stroke-width="1.8"/>`,
					x+jitter, y(v), color)
			}

			// Median value above the group.
			fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" font-size="10.5" fill="#344054" text-anchor="middle">%s</text>`,
				x, y(math.Max(hi, med))-7, format(med))
		}
	}

	// Legend.
	lx := left
	ly := h - 30.0
	for _, mode := range methodOrder {
		fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="14" height="14" fill="%s" rx="2"/>`, lx, ly-11, methodColor[mode])
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" font-size="12" fill="#344054">%s</text>`, lx+20, ly, methodLabel[mode])
		lx += 160
	}
	fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="3.2" fill="#ffffff" stroke="#344054" stroke-width="1.5"/>`, lx+4, ly-4)
	fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" font-size="12" fill="#344054">individual run</text>`, lx+16, ly)
	fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" font-size="11" fill="#667085" text-anchor="end">n = 3 repetitions per method and condition</text>`, w-right, ly)

	b.WriteString(`</svg>`)
	return b.String()
}

func niceCeil(v float64) float64 {
	if v <= 0 {
		return 1
	}
	mag := math.Pow(10, math.Floor(math.Log10(v)))
	n := v / mag
	switch {
	case n <= 1:
		n = 1
	case n <= 2:
		n = 2
	case n <= 2.5:
		n = 2.5
	case n <= 5:
		n = 5
	default:
		n = 10
	}
	return n * mag
}

func esc(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;")
	return r.Replace(s)
}

func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	cp := append([]float64(nil), values...)
	sort.Float64s(cp)
	m := len(cp) / 2
	if len(cp)%2 == 1 {
		return cp[m]
	}
	return (cp[m-1] + cp[m]) / 2
}

func minOf(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	m := values[0]
	for _, v := range values[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

func maxOf(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	m := values[0]
	for _, v := range values[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

func formatMS(v float64) string {
	if v >= 1000 {
		return fmt.Sprintf("%.1fs", v/1000)
	}
	if v >= 10 {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.1f", v)
}

func formatInt(v float64) string { return fmt.Sprintf("%.0f", v) }

func one(v float64) string  { return fmt.Sprintf("%.1f", v) }
func two(v float64) string  { return fmt.Sprintf("%.2f", v) }
func four(v float64) string { return fmt.Sprintf("%.4f", v) }
func atof(s string) float64 { v, _ := strconv.ParseFloat(s, 64); return v }
func atoi(s string) int     { v, _ := strconv.Atoi(s); return v }
