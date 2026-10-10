export default function HowItWorks() {
  const steps = [
    ["Validate", "Malformed, incomplete or unsupported-schema events go to the dead-letter table."],
    ["Stamp", "Each event is assigned to the 5-minute window in which the action happened (event_time)."],
    ["Deduplicate", "Repeated event_id values inside a one-hour horizon are ignored."],
    ["Aggregate", "Views, clicks, purchases, approximate distinct users and sessions per page."],
    ["Emit", "Preliminary rows about every 60 s, a final row at the watermark, corrections for 10 minutes."],
    ["Preserve", "Anything later than 10 minutes is written to the late-events table."],
  ];
  return (
    <>
      <header className="page-head">
        <p className="eyebrow">How it works</p>
        <h1>From click to dashboard</h1>
        <p className="sub">Because late data accumulates, a window can have several rows; this app shows the newest.</p>
      </header>
      <ol className="steps">
        {steps.map(([t, d], i) => (
          <li key={t} className="card step"><span className="step-n mono">{i + 1}</span>
            <div><h2>{t}</h2><p>{d}</p></div></li>
        ))}
      </ol>
      <div className="advisory"><strong>Reading the numbers.</strong> Distinct counts are approximate (about 3% error above 1,024 values per window and page).</div>
    </>
  );
}
