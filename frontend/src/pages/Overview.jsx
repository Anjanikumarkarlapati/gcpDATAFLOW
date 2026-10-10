import { useMemo, useState } from "react";
import Status from "../components/Status.jsx";
import Bars from "../components/Bars.jsx";
import { fmt, pct, hhmm } from "../data.js";

export default function Overview({ data }) {
  const [page, setPage] = useState("all");
  const pages = useMemo(() => [...new Set(data.metrics.map((m) => m.page_url))], [data]);
  const rows = data.metrics.filter((m) => page === "all" || m.page_url === page);

  const byWindow = useMemo(() => {
    const m = new Map();
    rows.forEach((r) => m.set(r.window_start, (m.get(r.window_start) || 0) + r.page_views));
    return [...m].map(([k, v]) => ({ label: hhmm(k), value: v }));
  }, [rows]);

  const sum = (k) => rows.reduce((a, r) => a + r[k], 0);
  const views = sum("page_views"), purchases = sum("purchases");
  const sessions = sum("sessions");
  const latest = [...rows].sort((a, b) => b.window_start.localeCompare(a.window_start) ||
    b.page_views - a.page_views).slice(0, 12);
  const deadRate = data.dead_total / data.totals.raw_events_sent;

  return (
    <>
      <header className="page-head">
        <p className="eyebrow">Overview · sample data</p>
        <h1>Engagement, five minutes at a time</h1>
        <p className="sub">Latest pane per window and page, as read from <code>page_metrics_5m_latest</code>.</p>
      </header>

      <section className="hero">
        <div className="hero-panel">
          <span className="hero-label">Page views, last hour</span>
          <span className="hero-number">{fmt.format(views)}</span>
          <Bars points={byWindow} label="Page views per five-minute window" />
        </div>
        <div className="tiles">
          <div className="tile"><span className="tile-label">Purchases</span>
            <span className="tile-value">{fmt.format(purchases)}</span></div>
          <div className="tile"><span className="tile-label">Conversion</span>
            <span className="tile-value">{pct(sessions ? purchases / sessions : null)}</span></div>
          <div className="tile"><span className="tile-label">Dead-letter rate</span>
            <span className="tile-value">{pct(deadRate)}</span>
            <Status tone={deadRate < 0.01 ? "good" : "warning"}>
              {deadRate < 0.01 ? "Under 1% target" : "Over 1% target"}</Status></div>
        </div>
      </section>

      <section className="card">
        <div className="card-head spread">
          <h2>Pages</h2>
          <label className="row">
            <span className="sr-only">Filter by page</span>
            <select className="input" value={page} onChange={(e) => setPage(e.target.value)}>
              <option value="all">All pages</option>
              {pages.map((p) => <option key={p}>{p}</option>)}
            </select>
          </label>
        </div>
        <div className="table-wrap">
          <table>
            <thead><tr><th>Window</th><th>Page</th><th className="num">Views</th>
              <th className="num">Clicks</th><th className="num">Users</th>
              <th className="num">Sessions</th><th className="num">Purchases</th>
              <th className="num">Conv.</th><th>State</th></tr></thead>
            <tbody>
              {latest.map((r) => (
                <tr key={r.window_start + r.page_url}>
                  <td className="mono">{hhmm(r.window_start)}–{hhmm(r.window_end)}</td>
                  <td className="mono">{r.page_url}</td>
                  <td className="num">{fmt.format(r.page_views)}</td>
                  <td className="num">{fmt.format(r.clicks)}</td>
                  <td className="num">{fmt.format(r.active_users)}</td>
                  <td className="num">{fmt.format(r.sessions)}</td>
                  <td className="num">{fmt.format(r.purchases)}</td>
                  <td className="num">{pct(r.conversion_rate)}</td>
                  <td>{r.is_final ? <Status tone="good">Final</Status> : <Status tone="info">Preliminary</Status>}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}
