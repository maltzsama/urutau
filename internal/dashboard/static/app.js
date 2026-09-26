// Dashboard SPA: Alpine.js components + Chart.js rendering. Live updates come
// from the coordinator's /api/v1/stream SSE endpoint (snapshot + deltas); the
// REST endpoints are a one-shot fallback. Charts are Chart.js instances kept
// per canvas and updated in place; the KPI sparklines share a generic
// mini-line config.
function cssVar(name, fallback) {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name);
  return (v && v.trim()) || fallback;
}

function app() {
  const API = '/api/v1';
  const HISTORY = 720; // 1h at a 5s sample
  const HISTORY_MS = 5000;
  const COLORS = ['#2563eb', '#16a34a', '#d97706', '#c0392b', '#7c3aed'];
// The attrs an operator scans for first when reading a log line: who, where
// and what failed lead the chip row; everything else follows alphabetically.
const LOG_LEAD = ['worker', 'table', 'err', 'error'];
  let es = null; // the EventSource, kept outside Alpine's reactive data
  const charts = {}; // Chart.js instances, kept outside Alpine's reactive data
  const samples = {}; // global series buffers (chart data, non-reactive)
  const series = {}; // per-stream series buffers (chart data, non-reactive)
  const streamColors = {}; // target -> color, assigned once so a stream never changes hue
  const workerTimeline = {}; // worker -> [{ t, status }] (non-reactive)
  let lastHistoryAt = 0;

  return {
    view: 'overview',
    theme: 'auto',
    summary: {},
    tables: [],
    workers: [],
    events: [],
    logs: [],
    streamFilter: '',
    workerFilter: '',
    sort: 'source',
    eventFilter: 'all',
    logFilter: 'all',
    eventLimit: 200,
    query: '',
    logQuery: '',
    paused: false,
    failures: 0,
    loaded: false,
    error: '',
    lastPoll: '',
    window: '30m',
    drawer: { open: false, target: '' },
    modal: { open: false, title: '', text: '', action: null, error: '' },
    toastMsg: '',

    tabs: [
      { id: 'overview', label: 'Overview' },
      { id: 'streams', label: 'Streams' },
      { id: 'workers', label: 'Workers' },
      { id: 'events', label: 'Events' },
      { id: 'logs', label: 'Logs' },
    ],

    init() {
      this.theme = localStorage.getItem('urutau-theme') || 'auto';
      this.applyTheme();
      window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => {
        if (this.theme === 'auto') this.applyTheme();
      });
      const v = new URLSearchParams(location.search).get('view');
      if (v) this.view = v;
      this.connectStream();
      window.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') { this.drawer.open = false; this.modal.open = false; }
      });
    },

    // connectStream subscribes to the coordinator's SSE stream: a full snapshot
    // on connect, then live 'state', 'event' and 'log' pushes. No polling. The
    // browser's EventSource reconnects on its own when the stream drops.
    connectStream() {
      if (es) es.close();
      es = new EventSource(API + '/stream');
      es.addEventListener('snapshot', (e) => {
        const s = JSON.parse(e.data);
        this.applyState(s);
        if (s.events) this.events = s.events;
        if (s.logs) this.logs = s.logs;
        this.loaded = true;
        this.error = '';
        this.failures = 0;
        this.lastPoll = new Date().toLocaleTimeString('en-GB');
        this.pushHistory();
        this.$nextTick(() => this.renderCharts());
      });
      es.addEventListener('state', (e) => {
        this.applyState(JSON.parse(e.data));
        this.lastPoll = new Date().toLocaleTimeString('en-GB');
        this.pushHistory();
        this.$nextTick(() => this.renderCharts());
      });
      es.addEventListener('event', (e) => {
        this.events.unshift(JSON.parse(e.data));
        if (this.events.length > 1000) this.events.pop();
      });
      es.addEventListener('log', (e) => {
        this.logs.unshift(JSON.parse(e.data));
        if (this.logs.length > 1000) this.logs.pop();
      });
      es.onerror = () => {
        // The stream dropped; the browser retries. Show the outage, keep the
        // last known state.
        this.failures++;
        this.error = 'Coordinator unreachable';
        this.lastPoll = 'stale';
      };
    },

    applyState(s) {
      if (s.pipeline) this.summary = s.pipeline;
      if (s.tables) this.tables = s.tables;
      if (s.workers) this.workers = s.workers;
    },

    // ── theme ─────────────────────────────────────────────────────────────
    applyTheme() {
      const dark = window.matchMedia('(prefers-color-scheme: dark)').matches;
      document.documentElement.setAttribute('data-theme',
        this.theme === 'auto' ? (dark ? 'dark' : 'light') : this.theme);
    },
    cycleTheme() {
      this.theme = this.theme === 'auto' ? 'light' : this.theme === 'light' ? 'dark' : 'auto';
      localStorage.setItem('urutau-theme', this.theme);
      this.applyTheme();
      this.$nextTick(() => this.renderCharts());
    },
    themeLabel() {
      return 'Theme: ' + this.theme;
    },

    // ── data ──────────────────────────────────────────────────────────────
    async fetchJSON(path) {
      const r = await fetch(path);
      if (!r.ok) throw new Error(path + ': ' + r.status);
      return r.json();
    },
    // refresh is a one-shot REST fetch, used by the Retry button. Live updates
    // arrive over the SSE stream, not here; each endpoint is fetched
    // independently so one failure cannot blank the whole dashboard.
    async refresh() {
      const [summary, tables, workers, events, logs] = await Promise.allSettled([
        this.fetchJSON(API + '/pipeline'),
        this.fetchJSON(API + '/tables'),
        this.fetchJSON(API + '/workers'),
        this.fetchJSON(API + '/events?limit=' + this.eventLimit),
        this.fetchJSON(API + '/logs?limit=500'),
      ]);
      if (summary.status === 'fulfilled') this.summary = summary.value || {};
      if (tables.status === 'fulfilled') this.tables = tables.value || [];
      if (workers.status === 'fulfilled') this.workers = workers.value || [];
      if (events.status === 'fulfilled') this.events = events.value || [];
      if (logs.status === 'fulfilled') this.logs = logs.value || [];
      if ([summary, tables, workers].every((r) => r.status === 'fulfilled')) {
        this.error = '';
        this.failures = 0;
        this.loaded = true;
        this.lastPoll = new Date().toLocaleTimeString('en-GB');
      } else {
        this.failures++;
        this.error = 'Coordinator unreachable';
        this.lastPoll = 'stale';
      }
      this.pushHistory();
      this.$nextTick(() => this.renderCharts());
    },
    pushHistory() {
      // Sample the series every 5s (not on every 2s poll), so the 720-point
      // ring spans ~1h.
      const now = Date.now();
      if (lastHistoryAt && now - lastHistoryAt < HISTORY_MS) return;
      lastHistoryAt = now;
      const ring = (obj, k, v) => {
        const h = obj[k] || (obj[k] = []);
        h.push(v);
        if (h.length > HISTORY) h.shift();
      };
      ring(samples, 'throughput', this.totalRate());
      ring(samples, 'lag', this.totalLag());
      ring(samples, 'commits', this.totalCommits());
      ring(samples, 'errors', this.errorCount());
      ring(samples, 'workers', this.attachedCount());
      ring(samples, 'snapshot', this.snapshotDone());

      for (const t of this.tables) {
        const s = series[t.target] || (series[t.target] = { lag: [], rate: [] });
        s.lag.push(t.lag_s || 0);
        s.rate.push(t.rows_rate || 0);
        if (s.lag.length > HISTORY) { s.lag.shift(); s.rate.shift(); }
      }
      for (const w of this.workers) {
        const tl = workerTimeline[w.name] || (workerTimeline[w.name] = []);
        if (!tl.length || tl[tl.length - 1].status !== w.status) {
          tl.push({ t: now, status: w.status });
          if (tl.length > 50) tl.shift();
        }
      }
    },

    // ── derived ───────────────────────────────────────────────────────────
    attachedCount() { return this.workers.filter((w) => w.status === 'attached').length; },
    workersOnline() { return this.attachedCount() + '/' + this.workers.length; },
    totalLag() { return this.tables.reduce((a, t) => a + (t.lag_s || 0), 0); },
    totalRate() { return this.tables.reduce((a, t) => a + (t.rows_rate || 0), 0); },
    totalCommits() { return this.tables.reduce((a, t) => a + (t.commits || 0), 0); },
    errorCount() { return this.events.filter((e) => this.eventClass(e) === 'error').length; },
    snapshotDone() { return this.tables.filter((t) => t.lag_s < 30).length; },
    maintenance() {
      const agg = { compaction: null, expiry: null, orphan: null };
      for (const t of this.tables) {
        const m = t.maintenance;
        if (!m) continue;
        for (const k of ['compaction', 'expiry', 'orphan']) {
          if (!m[k]) continue;
          if (!agg[k]) agg[k] = Object.assign({}, m[k]);
          else for (const f of Object.keys(m[k])) {
            if (typeof m[k][f] === 'number') agg[k][f] = (agg[k][f] || 0) + m[k][f];
          }
        }
      }
      return agg;
    },
    reclaimed() {
      const c = this.maintenance().compaction;
      return c ? Math.max(0, (c.bytes_before || 0) - (c.bytes_after || 0)) : 0;
    },
    bannerReason() {
      if (this.summary.status === 'failed') return 'The pipeline terminated.';
      if (this.summary.snapshot_active) return 'Snapshot in progress.';
      const bad = this.tables.filter((t) => t.lag_s > 30).length;
      const down = this.workers.filter((w) => w.status !== 'attached').length;
      if (!bad && !down) return 'All workers attached and lag is within threshold.';
      const parts = [];
      if (down) parts.push(down + ' worker(s) not attached');
      if (bad) parts.push(bad + ' stream(s) over the 30s lag threshold');
      return parts.join(' · ') + '.';
    },
    bannerClass() {
      if (this.summary.status === 'failed') return 'fail';
      const bad = this.tables.some((t) => t.lag_s > 30) || this.workers.some((w) => w.status !== 'attached');
      return bad ? 'warn' : '';
    },
    statusClass(s) {
      if (s === 'streaming') return 'green';
      if (s === 'starting' || s === 'snapshotting' || s === 'degraded' || s === 'stopping') return 'amber';
      if (s === 'failed') return 'red';
      return 'gray';
    },
    workerClass(w) {
      return w.status === 'attached' ? 'green' : w.status === 'pending' ? 'amber' : 'red';
    },
    streamStatus(t) {
      if (t.lag_s > 30) return 'degraded';
      if (t.snapshot_progress > 0 && t.snapshot_progress < 1) return 'snapshotting';
      return 'ok';
    },
    streamStatusClass(t) {
      const s = this.streamStatus(t);
      return s === 'ok' ? 'green' : s === 'degraded' ? 'red' : 'amber';
    },
    eventClass(e) {
      if (['schema_drift', 'worker_reset', 'job_terminated'].includes(e.type)) return 'error';
      if (e.type === 'delete_dropped') return 'warning';
      return '';
    },
    logClass(l) { return l.level === 'ERROR' ? 'error' : l.level === 'WARN' ? 'warning' : ''; },
    // logLevelClass colors the level pill; gray is debug noise, red is a
    // failure, amber is a warning.
    logLevelClass(l) {
      return l.level === 'ERROR' ? 'red' : l.level === 'WARN' ? 'amber' : l.level === 'DEBUG' ? 'gray' : '';
    },
    // logChips orders a record's attrs for display. Curated keys lead, the
    // rest sort alphabetically, so chips hold their place across updates
    // instead of reshuffling with the map's iteration order.
    logChips(l) {
      const attrs = l.attrs || {};
      const lead = (k) => { const i = LOG_LEAD.indexOf(k); return i < 0 ? LOG_LEAD.length : i; };
      return Object.keys(attrs).sort((a, b) => lead(a) - lead(b) || a.localeCompare(b))
        .map((k) => ({ k, v: attrs[k], err: k === 'err' || k === 'error' }));
    },
    drawerTable() { return this.tables.find((t) => t.target === this.drawer.target) || null; },
    workerTransitions(name) {
      return (workerTimeline[name] || []).slice(-5);
    },

    // streamOrder is the canonical, lag-independent order: source, then target
    // as the tiebreak. Everything stable (the chart's series selection, the
    // default list order) derives from it, so a changing lag can never reshuffle
    // cards or legends.
    streamOrder(a, b) {
      return a.source.localeCompare(b.source) || a.target.localeCompare(b.target);
    },
    filteredStreams() {
      let arr = this.tables.slice();
      if (this.streamFilter) arr = arr.filter((t) => (t.source + t.target).includes(this.streamFilter));
      if (this.sort === 'lag') {
        // Lag desc only on explicit request, with the same deterministic
        // tiebreak so equal lags never jitter between updates.
        arr.sort((a, b) => (b.lag_s - a.lag_s) || this.streamOrder(a, b));
      } else arr.sort(this.streamOrder);
      return arr;
    },
    // chartStreams picks the streams the overview lag chart shows. Selection
    // follows streamOrder — never the live lag ranking — so the set of series
    // (and therefore the legend) stays put while lag moves.
    chartStreams() {
      return this.tables.slice().sort(this.streamOrder).slice(0, 5);
    },
    // streamColor assigns a hue on first sight and keeps it forever, keyed by
    // the stream's target rather than its position in the dataset array.
    streamColor(target) {
      if (!(target in streamColors)) {
        streamColors[target] = COLORS[Object.keys(streamColors).length % COLORS.length];
      }
      return streamColors[target];
    },
    filteredWorkers() {
      return this.workers.filter((w) => !this.workerFilter || w.name.includes(this.workerFilter));
    },
    filteredEvents() {
      const q = this.query.toLowerCase();
      return this.events.filter((e) => {
        if (this.eventFilter === 'error' && this.eventClass(e) !== 'error') return false;
        if (this.eventFilter === 'warning' && this.eventClass(e) !== 'warning') return false;
        if (this.eventFilter === 'info' && this.eventClass(e) !== '') return false;
        return !q || JSON.stringify(e).toLowerCase().includes(q);
      });
    },
    filteredLogs() {
      const q = this.logQuery.toLowerCase();
      return this.logs.filter((l) => {
        if (this.logFilter === 'errors' && l.level !== 'ERROR') return false;
        if (this.logFilter === 'warnings' && !['WARN', 'ERROR'].includes(l.level)) return false;
        return !q || JSON.stringify(l).toLowerCase().includes(q);
      });
    },
    recentEvents() { return this.events.filter((e) => e.type !== 'commit').slice(0, 5); },

    // ── view / drawer / modal ─────────────────────────────────────────────
    setView(v) {
      this.view = v;
      history.replaceState(null, '', '?view=' + v);
      this.$nextTick(() => this.renderCharts());
    },
    openStream(t) {
      this.drawer = { open: true, target: t.target };
      this.$nextTick(() => this.renderDrawerChart());
    },
    openStreamByName(name) {
      const t = this.tables.find((x) => x.target === name || x.source === name);
      if (t) this.openStream(t);
    },
    confirmCancel() {
      this.openModal('Cancel pipeline',
        'This sends Shutdown to every worker and terminates the pipeline.',
        async () => {
          await this.post(API + '/actions/cancel');
          this.toast('Shutdown requested');
        });
    },
    confirmRestart(name) {
      this.openModal('Restart ' + name + '?',
        'The epoch is bumped and the session is reset.',
        async () => {
          await this.post(API + '/actions/restart/' + encodeURIComponent(name));
          this.toast('Restart requested for ' + name);
        });
    },
    async post(path) {
      const r = await fetch(path, { method: 'POST' });
      if (!r.ok) throw new Error(await r.text());
    },
    openModal(title, text, action) {
      this.modal = { open: true, title, text, action, error: '' };
    },
    async runModalAction() {
      try {
        if (this.modal.action) await this.modal.action();
        this.modal.open = false;
        this.refresh();
      } catch (e) {
        this.modal.error = String(e);
      }
    },
    toast(msg) {
      this.toastMsg = msg;
      setTimeout(() => { this.toastMsg = ''; }, 2200);
    },
    copy(s) {
      if (!s) return;
      const text = String(s);
      const ok = () => this.toast('Copied');
      const bad = () => this.toast('Copy unavailable');
      // navigator.clipboard is only available in a secure context (HTTPS or
      // localhost); on plain HTTP fall back to a temp textarea + execCommand.
      if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(text).then(ok).catch(bad);
        return;
      }
      const ta = document.createElement('textarea');
      ta.value = text;
      ta.setAttribute('readonly', '');
      ta.style.cssText = 'position:fixed;top:-9999px;left:-9999px;';
      document.body.appendChild(ta);
      ta.select();
      ta.setSelectionRange(0, text.length);
      try {
        document.execCommand('copy') ? ok() : bad();
      } catch (_) {
        bad();
      }
      document.body.removeChild(ta);
    },

    // ── formatting ────────────────────────────────────────────────────────
    fmt(n) {
      n = n || 0;
      return n >= 1e6 ? (n / 1e6).toFixed(1) + 'M' : n >= 1e3 ? (n / 1e3).toFixed(1) + 'k' : String(n);
    },
    bytes(n) {
      n = n || 0;
      return n >= 1e9 ? (n / 1e9).toFixed(1) + ' GB'
        : n >= 1e6 ? (n / 1e6).toFixed(1) + ' MB'
        : n >= 1e3 ? (n / 1e3).toFixed(1) + ' KB' : n + ' B';
    },
    ts(s) { return s ? new Date(s).toLocaleString('en-GB', { hour12: false }) : '—'; },
    ago(s) {
      if (!s) return '—';
      const d = (Date.now() - new Date(s).getTime()) / 1000;
      return d < 60 ? Math.round(d) + 's ago' : d < 3600 ? Math.round(d / 60) + 'm ago' : Math.round(d / 3600) + 'h ago';
    },
    uptime() {
      const s = this.summary.uptime_s || 0;
      return s >= 3600 ? Math.floor(s / 3600) + 'h ' + Math.floor((s % 3600) / 60) + 'm'
        : s >= 60 ? Math.floor(s / 60) + 'm ' + (s % 60) + 's' : s + 's';
    },
    ms(n) { return n ? n.toFixed(0) + ' ms' : '—'; },

    // ── charts (Chart.js) ─────────────────────────────────────────────────
    windowSeconds() {
      return this.window === '5m' ? 300 : this.window === '1h' ? 3600 : 1800;
    },
    // tail caps a series to the client-side ring size. The visible window is
    // set by the x-axis domain (see line/drawerChart), not by slicing here, so
    // changing the window zooms the chart even before a full window of data
    // has accumulated.
    tail(arr) {
      return arr && arr.length > HISTORY ? arr.slice(arr.length - HISTORY) : (arr || []);
    },
    // points maps a value series to {x,y} points on a time axis anchored at
    // "now" (x=0), stepping back one sample interval per point.
    points(arr) {
      const a = arr || [];
      const stepS = HISTORY_MS / 1000;
      return a.map((v, i) => ({ x: -(a.length - 1 - i) * stepS, y: v }));
    },
    renderCharts() {
      if (typeof Chart === 'undefined') return;
      const grid = cssVar('--pico-muted-border-color', '#ddd');
      if (this.view === 'overview') {
        this.line('chart-throughput', [this.tail(samples.throughput)], ['Throughput'], grid);
        const streams = this.chartStreams();
        this.line('chart-lag',
          streams.map((t) => this.tail((series[t.target] || {}).lag)),
          streams.map((t) => t.source + ' → ' + t.target), grid,
          streams.map((t) => this.streamColor(t.target)));
      }
      for (const id of ['spark-throughput', 'spark-lag', 'spark-commits', 'spark-errors', 'spark-workers', 'spark-snapshot']) {
        const key = id.replace('spark-', '');
        this.spark(id, this.tail(samples[key]));
      }
      if (this.drawer.open) this.renderDrawerChart();
    },
    line(id, datasets, labels, grid, colors) {
      const el = document.getElementById(id);
      if (!el) return;
      const data = {
        datasets: datasets.map((d, i) => ({
          label: labels[i], data: this.points(d),
          borderColor: (colors && colors[i]) || COLORS[i % COLORS.length],
          backgroundColor: 'transparent', tension: 0.3, pointRadius: 0, borderWidth: 2,
        })),
      };
      const opts = {
        responsive: true, maintainAspectRatio: false, animation: false,
        plugins: { legend: { display: datasets.length > 1, labels: { boxWidth: 10 } } },
        scales: {
          x: { type: 'linear', min: -this.windowSeconds(), max: 0, display: false },
          y: { beginAtZero: true, grid: { color: grid },
               ticks: { color: cssVar('--pico-muted-color', '#888'), maxTicksLimit: 5, font: { size: 10 } } },
        },
      };
      if (charts[id]) {
        charts[id].data = data;
        charts[id].options = opts;
        charts[id].update('none');
      } else {
        charts[id] = new Chart(el, { type: 'line', data, options: opts });
      }
    },
    spark(id, data) {
      const el = document.getElementById(id);
      if (!el) return;
      const cfg = {
        labels: (data || []).map((_, i) => i),
        datasets: [{ data: data || [], borderColor: COLORS[0], backgroundColor: 'transparent', tension: 0.3, pointRadius: 0, borderWidth: 1.8 }],
      };
      const opts = {
        responsive: true, maintainAspectRatio: false, animation: false,
        plugins: { legend: { display: false } },
        scales: { x: { display: false }, y: { display: false } },
      };
      if (charts[id]) {
        charts[id].data = cfg;
        charts[id].update('none');
      } else {
        charts[id] = new Chart(el, { type: 'line', data: cfg, options: opts });
      }
    },
    renderDrawerChart() {
      if (typeof Chart === 'undefined') return;
      const el = document.getElementById('chart-drawer');
      if (!el) return;
      const s = series[this.drawer.target] || {};
      const rate = this.tail(s.rate);
      const lag = this.tail(s.lag);
      const data = {
        datasets: [
          { label: 'rows/s', data: this.points(rate), borderColor: COLORS[0], backgroundColor: 'transparent', tension: 0.3, pointRadius: 0, borderWidth: 2, yAxisID: 'y' },
          { label: 'lag (s)', data: this.points(lag), borderColor: COLORS[2], backgroundColor: 'transparent', tension: 0.3, pointRadius: 0, borderWidth: 2, yAxisID: 'y1' },
        ],
      };
      const opts = {
        responsive: true, maintainAspectRatio: false, animation: false,
        plugins: { legend: { display: true, labels: { boxWidth: 10 } } },
        scales: {
          x: { type: 'linear', min: -this.windowSeconds(), max: 0, display: false },
          // Two series, two units: label each axis and color it to match its
          // line so the two number scales are never ambiguous.
          y: { position: 'left', beginAtZero: true,
               title: { display: true, text: 'rows/s', color: COLORS[0], font: { size: 10 } },
               grid: { color: cssVar('--pico-muted-border-color', '#ddd') },
               ticks: { color: COLORS[0], maxTicksLimit: 5, font: { size: 10 } } },
          y1: { position: 'right', beginAtZero: true,
                title: { display: true, text: 'lag (s)', color: COLORS[2], font: { size: 10 } },
                grid: { drawOnChartArea: false },
                ticks: { color: COLORS[2], maxTicksLimit: 5, font: { size: 10 } } },
        },
      };
      if (charts['chart-drawer']) {
        charts['chart-drawer'].data = data;
        charts['chart-drawer'].update('none');
      } else {
        charts['chart-drawer'] = new Chart(el, { type: 'line', data, options: opts });
      }
    },
  };
}
window.app = app;
