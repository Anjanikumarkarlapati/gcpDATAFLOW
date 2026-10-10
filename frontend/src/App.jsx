import { NavLink, Route, Routes } from "react-router-dom";
import { useEffect, useState } from "react";
import { ChartBar, Moon, Sun, WarningDiamond, BookOpen } from "@phosphor-icons/react";
import Overview from "./pages/Overview.jsx";
import Quality from "./pages/Quality.jsx";
import HowItWorks from "./pages/HowItWorks.jsx";
import { useData } from "./data.js";

function useTheme() {
  const [theme, setTheme] = useState(() => document.documentElement.getAttribute("data-theme") || "light");
  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    try { localStorage.setItem("theme", theme); } catch (e) { /* ignore */ }
  }, [theme]);
  return [theme, () => setTheme((t) => (t === "dark" ? "light" : "dark"))];
}

export default function App() {
  const [theme, toggle] = useTheme();
  const { data, error } = useData();
  const link = ({ isActive }) => (isActive ? "nav-link active" : "nav-link");
  return (
    <>
      <header className="topbar">
        <span className="wordmark">Clickstream Pulse</span>
        <nav aria-label="Primary">
          <NavLink to="/" end className={link}><ChartBar size={18} aria-hidden="true" /><span>Overview</span></NavLink>
          <NavLink to="/quality" className={link}><WarningDiamond size={18} aria-hidden="true" /><span>Data quality</span></NavLink>
          <NavLink to="/how" className={link}><BookOpen size={18} aria-hidden="true" /><span>How it works</span></NavLink>
        </nav>
        <button className="btn btn-quiet btn-icon" onClick={toggle}
          aria-label={theme === "dark" ? "Switch to light theme" : "Switch to dark theme"}>
          {theme === "dark" ? <Sun size={20} /> : <Moon size={20} />}
        </button>
      </header>
      <main className="page">
        {error && <div className="banner banner-critical" role="alert">Could not load data: {error.message}</div>}
        {!data && !error && <p className="empty">Loading…</p>}
        {data && (
          <Routes>
            <Route path="/" element={<Overview data={data} />} />
            <Route path="/quality" element={<Quality data={data} />} />
            <Route path="/how" element={<HowItWorks />} />
          </Routes>
        )}
      </main>
    </>
  );
}
