import { useEffect, useState } from "react";

// Sample data produced by scripts/generate_sample_data.py (real pipeline code).
// Swap this URL for an API in front of page_metrics_5m_latest for live data.
export function useData() {
  const [state, setState] = useState({ data: null, error: null });
  useEffect(() => {
    fetch(`${import.meta.env.BASE_URL}sample-data.json`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((data) => setState({ data, error: null }))
      .catch((error) => setState({ data: null, error }));
  }, []);
  return state;
}

export const fmt = new Intl.NumberFormat("en-US");
export const pct = (v) => (v == null ? "—" : `${(v * 100).toFixed(1)}%`);
export const hhmm = (s) => s.slice(11, 16);
