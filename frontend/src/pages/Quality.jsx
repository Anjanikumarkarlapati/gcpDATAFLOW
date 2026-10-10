import { useState } from "react";
import Status from "../components/Status.jsx";
import { fmt, hhmm } from "../data.js";

export default function Quality({ data }) {
  const [tab, setTab] = useState("dead");
  return (
    <>
      <header className="page-head">
        <p className="eyebrow">Data quality</p>
        <h1>Dead letters and late events</h1>
        <p className="sub">Nothing is silently dropped: rejected and too-late events are kept for debugging and replay.</p>
      </header>
      <div className="banner banner-warning" role="note">
        <Status tone="warning">Sample data</Status>
        <span>Figures below come from a generated hour of traffic with injected bad and stale events.</span>
      </div>
      <div className="tabs" role="tablist">
        <button role="tab" aria-selected={tab === "dead"} className={tab === "dead" ? "tab active" : "tab"}
          onClick={() => setTab("dead")}>Dead letters ({fmt.format(data.dead_total)})</button>
        <button role="tab" aria-selected={tab === "late"} className={tab === "late" ? "tab active" : "tab"}
          onClick={() => setTab("late")}>Late events ({fmt.format(data.late_total)})</button>
      </div>
      <section className="card">
        <div className="table-wrap">
          {tab === "dead" ? (
            <table>
              <thead><tr><th>Type</th><th>Reason</th><th>Raw payload</th></tr></thead>
              <tbody>{data.dead_letters.map((r, i) => (
                <tr key={i}><td><span className="tag">{r.failure_type}</span></td>
                  <td>{r.failure_reason}</td><td className="mono clip">{r.raw_payload}</td></tr>))}
              </tbody>
            </table>
          ) : (
            <table>
              <thead><tr><th>Event ID</th><th>Page</th><th>Event time</th><th>Window end</th>
                <th className="num">Late by</th></tr></thead>
              <tbody>{data.late_events.map((r) => (
                <tr key={r.event_id}><td className="mono clip">{r.event_id}</td>
                  <td className="mono">{r.page_url}</td><td className="mono">{hhmm(r.event_time)}</td>
                  <td className="mono">{hhmm(r.window_end)}</td>
                  <td className="num">{Math.round(r.lateness_seconds / 60)} min</td></tr>))}
              </tbody>
            </table>
          )}
        </div>
      </section>
    </>
  );
}
