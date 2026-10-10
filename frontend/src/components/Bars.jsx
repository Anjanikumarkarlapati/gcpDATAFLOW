// Page views per 5-minute window as accessible SVG bars.
export default function Bars({ points, label }) {
  const max = Math.max(...points.map((p) => p.value), 1);
  const w = 100 / points.length;
  return (
    <figure className="bars">
      <svg viewBox="0 0 100 40" preserveAspectRatio="none" role="img" aria-label={label}>
        {points.map((p, i) => (
          <rect key={p.label} x={i * w + w * 0.12} width={w * 0.76}
            y={40 - (p.value / max) * 38} height={(p.value / max) * 38} rx="0.6">
            <title>{`${p.label}: ${p.value}`}</title>
          </rect>
        ))}
      </svg>
      <figcaption className="bars-axis">
        <span>{points[0].label}</span>
        <span>{points[points.length - 1].label}</span>
      </figcaption>
    </figure>
  );
}
