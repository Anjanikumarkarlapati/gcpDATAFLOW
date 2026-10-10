import { CheckCircle, Warning, XCircle, Info, Circle } from "@phosphor-icons/react";

// Status is never colour alone: colour + icon shape + word.
const TONES = {
  good: { Icon: CheckCircle, label: "Healthy" },
  warning: { Icon: Warning, label: "Attention" },
  critical: { Icon: XCircle, label: "Critical" },
  info: { Icon: Info, label: "Info" },
  muted: { Icon: Circle, label: "Idle" },
};

export default function Status({ tone = "muted", children }) {
  const { Icon, label } = TONES[tone];
  return (
    <span className={`status status-${tone}`}>
      <Icon size={16} weight="bold" aria-hidden="true" />
      {children ?? label}
    </span>
  );
}
