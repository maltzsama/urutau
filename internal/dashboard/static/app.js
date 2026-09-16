// Dashboard SPA: Alpine.js components + Chart.js rendering. All data comes
// from the coordinator's /api/v1/* endpoints (client-side polling; no
// WebSocket). Charts are Chart.js instances kept per canvas and updated in
// place; KPI sparklines share a generic mini-line config.
function app() {
  const API = '/api/v1';
  const HISTORY = 720; // 1h at a 5s sample
  const HISTORY_MS = 5000;

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
    sort: 'lag',
    eventFilter: 'all',
    logFilter: 'all',
    eventLimit: 200,
    query: '',
    paused: false,
    failures: 0,
    lastPoll: '',
    window: '30m',
    drawer: { open: false, title: '', table: null },
    modal: { open: false, title: '', text: '', action: null, error: '' },
    toastMsg: '',
    charts: {},
    history: {},
    lastHistoryAt: 0,
    timers: [],

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
      this.refresh();
      this.timers.push(setInterval(() => this.refresh(), 2000));
      window.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') { this.drawer.open = false; this.modal.open = false; }
      });
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
      return this.theme === 'auto' ? '◐ auto' : this.theme === 'light' ? '☀ light' : '☾ dark';
    },

    // ── data ──────────────────────────────────────────────────────────────
    async fetchJSON(path) {
      const r = await fetch(path);
      if (!r.ok) throw new Error(path + ': ' + r.status);
      return r.json();
    },
    async refresh() {
      try {
        const [summary, tables, workers, events, logs] = await Promise.all([
          this.fetchJSON(API + '/pipeline'),
          this.fetchJSON(API + '/tables'),
          this.fetchJSON(API + '/workers'),
          this.fetchJSON(API + '/events?limit=' + this.eventLimit),
          this.fetchJSON(API + '/logs?limit=500'),
        ]);
        this.summary = summary || {};
        this.tables = tables || [];
        this.workers = workers || [];
        this.events = events || [];
        this.logs = logs || [];
        this.failures = 0;
        this.lastPoll = new Date().toLocaleTimeString('en-GB');
        this.pushHistory();
        this.$nextTick(() => this.renderCharts());
      } catch (e) {
        this.failures++;
        this.lastPoll = 'stale';
      }
    },
    pushHistory() {
      // Sample the series every 5s (not on every 2s poll), so the 720-point
      // ring spans ~1h.
      const now = Date.now();
      if (this.lastHistoryAt && now - this.lastHistoryAt < HISTORY_MS) return;
      this.lastHistoryAt = now;
      const push = (k, v) => {
        const h = this.history[k] || (this.history[k] = []);
        h.push(v);
        if (h.length > HISTORY) h.shift();
      };
      push('throughput', this.totalRate());
      push('lag', this.totalLag());
      push('commits', this.totalCommits());
      push('errors', this.errorCount());
      push('workers', this.workersOnline());
      push('snapshot', this.snapshotDone());
    },

    // ── derived ───────────────────────────────────────────────────────────
    workersOnline() {
      return this.workers.filter((w) => w.status === 'attached').length + '/' + this.workers.length;
    },
    totalLag() { return this.tables.reduce((a, t) => a + (t.lag_s || 0), 0); },
    totalRate() { return this.tables.reduce((a, t) => a + (t.rows_rate || 0), 0); },
    totalCommits() { return this.tables.reduce((a, t) => a + (t.commits || 0), 0); },
    errorCount() { return this.events.filter((e) => e.severity === 'error' || e.type === 'schema_drift' || e.type === 'worker_reset' || e.type === 'job_terminated').length; },
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
    streamStatus(t) { return t.lag_s > 30 ? 'degraded' : 'ok'; },
    streamStatusClass(t) { return t.lag_s > 30 ? 'amber' : 'green'; },
    eventClass(e) {
      if (['schema_drift', 'worker_reset', 'job_terminated'].includes(e.type)) return 'error';
      if (e.type === 'delete_dropped') return 'warning';
      return '';
    },
    logClass(l) { return l.level === 'ERROR' ? 'error' : l.level === 'WARN' ? 'warning' : ''; },

    filteredStreams() {
      let arr = this.tables.slice();
      if (this.streamFilter) arr = arr.filter((t) => (t.source + t.target).includes(this.streamFilter));
      if (this.sort === 'lag') arr.sort((a, b) => b.lag_s - a.lag_s);
      else arr.sort((a, b) => a.source.localeCompare(b.source));
      return arr;
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
      const q = this.query.toLowerCase();
      return this.logs.filter((l) => {
        if (this.logFilter === 'errors' && l.level !== 'ERROR') return false;
        if (this.logFilter === 'warnings' && !['WARN', 'ERROR'].includes(l.level)) return false;
        return !q || JSON.stringify(l).toLowerCase().includes(q);
      });
    },
    recentEvents() { return this.events.slice(0, 5); },

    // ── view / drawer / modal ─────────────────────────────────────────────
    setView(v) {
      this.view = v;
      history.replaceState(null, '', '?view=' + v);
      this.$nextTick(() => this.renderCharts());
    },
    openStream(t) {
      this.drawer = { open: true, title: t.source + ' → ' + t.target, table: t };
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
          await this.fetchJSON(API + '/actions/cancel').catch((e) => { throw e; });
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
      navigator.clipboard?.writeText(String(s))
        .then(() => this.toast('Copied'))
        .catch(() => this.toast('Copy unavailable'));
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

    // ── charts (Chart.js) ─────────────────────────────────────────────────
    renderCharts() {
      if (typeof Chart === 'undefined') return;
      const grid = getComputedStyle(document.documentElement).getPropertyValue('--pico-muted-border-color') || '#ddd';
      if (this.view === 'overview') {
        this.line('chart-throughput', 'throughput', [this.history.throughput || []], ['Throughput'], grid);
        const series = this.filteredStreams().slice(0, 5).map((t) => ({ name: t.source, data: [t.lag_s] }));
        this.line('chart-lag', 'lag', series.map((s) => s.data), series.map((s) => s.name), grid);
      }
      for (const id of ['spark-throughput', 'spark-lag', 'spark-commits', 'spark-errors', 'spark-workers', 'spark-snapshot']) {
        const key = id.replace('spark-', '');
        this.spark(id, this.history[key] || []);
      }
    },
    line(id, kind, datasets, labels, grid) {
      const el = document.getElementById(id);
      if (!el) return;
      const data = {
        labels: datasets[0] ? datasets[0].map((_, i) => i) : [],
        datasets: datasets.map((d, i) => ({
          label: labels[i], data: d, borderColor: ['#2563eb', '#16a34a', '#d97706', '#c0392b', '#7c3aed'][i % 5],
          backgroundColor: 'transparent', tension: 0.3, pointRadius: 0, borderWidth: 2,
        })),
      };
      const opts = {
        responsive: true, maintainAspectRatio: false, animation: false,
        plugins: { legend: { display: datasets.length > 1, labels: { boxWidth: 10 } } },
        scales: {
          x: { display: false },
          y: { grid: { color: grid }, ticks: { color: grid } },
        },
      };
      if (this.charts[id]) {
        this.charts[id].data = data;
        this.charts[id].update('none');
      } else {
        this.charts[id] = new Chart(el, { type: 'line', data, options: opts });
      }
    },
    spark(id, data) {
      const el = document.getElementById(id);
      if (!el) return;
      const cfg = {
        labels: data.map((_, i) => i),
        datasets: [{ data, borderColor: '#2563eb', backgroundColor: 'transparent', tension: 0.3, pointRadius: 0, borderWidth: 1.8 }],
      };
      const opts = {
        responsive: true, maintainAspectRatio: false, animation: false,
        plugins: { legend: { display: false } },
        scales: { x: { display: false }, y: { display: false } },
      };
      if (this.charts[id]) {
        this.charts[id].data = cfg;
        this.charts[id].update('none');
      } else {
        this.charts[id] = new Chart(el, { type: 'line', data: cfg, options: opts });
      }
    },
    renderDrawerChart() {
      if (typeof Chart === 'undefined' || !this.drawer.table) return;
      const el = document.getElementById('chart-drawer');
      if (!el) return;
      const h = this.history.throughput || [];
      if (this.charts['chart-drawer']) {
        this.charts['chart-drawer'].data.datasets[0].data = h;
        this.charts['chart-drawer'].update('none');
        return;
      }
      this.charts['chart-drawer'] = new Chart(el, {
        type: 'line',
        data: { labels: h.map((_, i) => i), datasets: [{ data: h, borderColor: '#2563eb', backgroundColor: 'transparent', tension: 0.3, pointRadius: 0, borderWidth: 2 }] },
        options: { responsive: true, maintainAspectRatio: false, animation: false, plugins: { legend: { display: false } }, scales: { x: { display: false }, y: { display: true } } },
      });
    },
  };
}
window.app = app;
